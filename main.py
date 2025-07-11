# main.py
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, delete, func, update
from sqlalchemy.orm import selectinload

from database import get_db, engine, Base
from models import Team, RankingSnapshot, RankingHistory, TeamPlayer, Match, Tournament, TeamMatchInfo
import crud
import schemas
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importações condicionais para o sistema de ranking
try:
    from ranking import calculate_ranking, RankingCalculator
    from ranking_history import save_ranking_snapshot, get_team_history
    import pandas as pd  # Add pandas import
    RANKING_AVAILABLE = True
    logger.info("✅ Sistema de ranking carregado com sucesso")
except ImportError as e:
    logger.warning(f"⚠️ Sistema de ranking não disponível: {e}")
    RANKING_AVAILABLE = False
    
    async def save_ranking_snapshot(db): 
        return None
    
    async def get_team_history(db, team_id, limit): 
        return []
    
    async def calculate_ranking(db, include_variation=True):
        return []

# Cache do ranking
ranking_cache = {
    "data": None,
    "timestamp": None,
    "ttl": timedelta(hours=1)
}

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        
        # Headers de segurança
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        return response

# Configuração da API
app = FastAPI(
    title="Valorant Universitário API",
    version="1.0.0",
    docs_url="/docs",
    description="API para consultar dados de partidas do Valorant Universitário",
    openapi_tags=[
        {"name": "root", "description": "Endpoints principais"},
        {"name": "teams", "description": "Operações com times"},
        {"name": "tournaments", "description": "Operações com torneios"},
        {"name": "matches", "description": "Operações com partidas"},
        {"name": "ranking", "description": "Sistema de ranking"},
        {"name": "stats", "description": "Estatísticas gerais"},
        {"name": "players", "description": "Operações com jogadores"},
        {"name": "admin", "description": "Operações administrativas"},
        {"name": "debug", "description": "Endpoints de debug"}
    ]
)

# Middleware de segurança
app.add_middleware(SecurityHeadersMiddleware)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ════════════════════════════════ ROOT ════════════════════════════════

@app.get("/", tags=["root"])
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "API Valorant Universitário",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "online"
    }

@app.get("/health", response_class=PlainTextResponse, tags=["root"])
@app.head("/health", response_class=PlainTextResponse, include_in_schema=False)
async def health_check():
    """
    Health check endpoint para monitoramento.
    Não toca no banco e devolve 200 a GET ou HEAD em <1 ms.
    """
    return PlainTextResponse("OK", status_code=200)

# ════════════════════════════════ TEAMS ════════════════════════════════

@app.get("/teams", response_model=List[schemas.Team], tags=["teams"])
async def list_teams(db: AsyncSession = Depends(get_db)):
    """Lista todos os times ordenados alfabeticamente"""
    return await crud.list_teams(db)

@app.get("/teams/search", response_model=List[schemas.Team], tags=["teams"])
async def search_teams(
    q: str = Query(None, description="Buscar por nome, slug ou tag"),
    university: str = Query(None, description="Filtrar por universidade"),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Busca times com filtros"""
    return await crud.search_teams(db, query=q, university=university, limit=limit)

@app.get("/teams/by-slug/{slug}", tags=["teams"])
async def get_team_by_slug(slug: str, db: AsyncSession = Depends(get_db)):
    """Busca um time pelo slug"""
    stmt = select(Team).where(Team.slug == slug)
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    return {
        "id": team.id,
        "name": team.name,
        "logo": team.logo,
        "tag": team.tag,
        "slug": team.slug,
        "university": team.university,
        "university_tag": team.university_tag,
        "estado": team.estado  # NOVO CAMPO
    }

@app.get("/teams/{team_id}", response_model=schemas.Team, tags=["teams"])
async def get_team(team_id: int, db: AsyncSession = Depends(get_db)):
    """Retorna detalhes de um time específico"""
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    return team

@app.get("/teams/{team_id}/matches", response_model=List[schemas.Match], tags=["teams"])
async def get_team_matches(
    team_id: int,
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Retorna todas as partidas de um time"""
    try:
        # Verifica se o time existe
        team = await crud.get_team(db, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Time não encontrado")
        
        # Log para debug
        logger.info(f"Buscando partidas do time {team_id}: {team.name}")
        
        # Busca partidas com tratamento de erro
        try:
            matches = await crud.get_team_matches(db, team_id, limit)
            logger.info(f"Encontradas {len(matches)} partidas para o time {team_id}")
            return matches
        except Exception as e:
            logger.error(f"Erro ao buscar partidas do time {team_id}: {str(e)}")
            logger.error(f"Tipo do erro: {type(e).__name__}")
            
            # Se for erro de serialização do Pydantic, tenta identificar o campo problemático
            if "validation error" in str(e).lower():
                logger.error("Possível erro de validação do Pydantic")
                
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            raise HTTPException(
                status_code=500, 
                detail=f"Erro ao processar partidas do time: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

@app.get("/teams/{team_id}/stats", tags=["teams"])
async def get_team_stats(team_id: int, db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas de vitórias/derrotas de um time"""
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    stats = await crud.get_team_stats(db, team_id)
    stats["team"] = {
        "id": team.id,
        "name": team.name,
        "tag": team.tag,
        "university": team.university,
        "estado": team.estado  # NOVO CAMPO
    }
    return stats

@app.get("/teams/{team_id}/players", tags=["teams", "players"])
async def get_team_players(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna os jogadores de um time específico"""
    
    # Verifica se o time existe
    team_stmt = select(Team).where(Team.id == team_id)
    team_result = await db.execute(team_stmt)
    team = team_result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca os jogadores
    players_stmt = (
        select(TeamPlayer.player_nick, TeamPlayer.id)
        .where(TeamPlayer.team_id == team_id)
        .order_by(TeamPlayer.id)
    )
    players_result = await db.execute(players_stmt)
    players = [{"nick": row[0], "id": row[1]} for row in players_result]
    
    return players

@app.get("/teams/{team_id}/history", tags=["teams", "ranking"])
async def get_team_ranking_history_old(
    team_id: int,
    limit: int = Query(100, ge=1, le=365),
    db: AsyncSession = Depends(get_db)
):
    """Retorna o histórico de ranking de um time (deprecated - use /ranking/team/{team_id}/history)"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    history = await get_team_history(db, team_id, limit)
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "university": team.university
        },
        "history": history,
        "count": len(history)
    }

@app.get("/teams/{team_id}/tournaments", tags=["teams", "tournaments"])
async def get_team_tournaments(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna todos os torneios que o time participou com estatísticas detalhadas"""
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca torneios
    tournaments_data = await crud.get_team_tournaments(db, team_id)
    
    tournaments = []
    for row in tournaments_data:
        win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
        
        # Determina status do torneio
        status = "finished"
        now = datetime.now(timezone.utc)
        
        if row.ends_on:
            ends_on_utc = row.ends_on.replace(tzinfo=timezone.utc) if row.ends_on.tzinfo is None else row.ends_on
            if ends_on_utc > now:
                status = "active"
        elif row.last_match:
            # Se não tem data de fim, verifica última partida
            last_match_utc = row.last_match.replace(tzinfo=timezone.utc) if row.last_match.tzinfo is None else row.last_match
            if (now - last_match_utc).days < 30:
                status = "active"
        
        tournaments.append({
            "tournament": {
                "id": str(row.id),
                "name": row.name,
                "logo": row.logo,
                "organizer": row.organizer,
                "starts_on": row.starts_on.isoformat() if row.starts_on else None,
                "ends_on": row.ends_on.isoformat() if row.ends_on else None,
            },
            "performance": {
                "matches_played": row.matches_played,
                "wins": row.wins,
                "losses": row.total_matches - row.wins,
                "win_rate": round(win_rate, 1)
            },
            "participation": {
                "first_match": row.first_match.isoformat() if row.first_match else None,
                "last_match": row.last_match.isoformat() if row.last_match else None,
                "status": status
            }
        })
    
    # Separa torneios ativos e finalizados
    active_tournaments = [t for t in tournaments if t["participation"]["status"] == "active"]
    finished_tournaments = [t for t in tournaments if t["participation"]["status"] == "finished"]
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "university": team.university
        },
        "summary": {
            "total_tournaments": len(tournaments),
            "active_tournaments": len(active_tournaments),
            "finished_tournaments": len(finished_tournaments)
        },
        "active": active_tournaments,
        "finished": finished_tournaments
    }


@app.get("/teams/{team_id}/complete", tags=["teams"])
async def get_team_complete_info(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint otimizado que retorna TODAS as informações necessárias
    para construir a página completa de uma equipe em uma única chamada.
    """
    
    try:
        # 1. Dados básicos da equipe
        team = await crud.get_team(db, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Time não encontrado")
        
        # 2. Jogadores
        players_stmt = (
            select(TeamPlayer.player_nick, TeamPlayer.id)
            .where(TeamPlayer.team_id == team_id)
            .order_by(TeamPlayer.id)
        )
        players_result = await db.execute(players_stmt)
        players = [{"nick": row[0], "id": row[1]} for row in players_result]
        
        # 3. Ranking atual e histórico (se disponível)
        current_ranking = None
        ranking_history = []
        
        if RANKING_AVAILABLE:
            try:
                # Posição atual no ranking
                ranking_data = await calculate_ranking(db, include_variation=True)
                for item in ranking_data:
                    if item.get("team_id") == team_id:
                        current_ranking = {
                            "position": item["posicao"],
                            "nota_final": item["nota_final"],
                            "variation": item.get("variacao"),
                            "is_new": item.get("is_new", False),
                            "games_count": item["games_count"],
                            "incerteza": item["incerteza"],
                            "scores": item["scores"]
                        }
                        break
                
                # Histórico (últimos 10 snapshots)
                history_data = await get_team_history(db, team_id, limit=10)
                ranking_history = history_data
            except Exception as e:
                logger.warning(f"Erro ao buscar ranking do time {team_id}: {e}")
        
        # 4. Estatísticas
        stats = await crud.get_team_stats(db, team_id)
        
        # 5. Partidas recentes (últimas 10)
        recent_matches = await crud.get_team_matches(db, team_id, limit=10)
        
        # 6. Torneios
        tournaments_data = await crud.get_team_tournaments(db, team_id)
        tournaments = []
        
        if tournaments_data:  # Handle None case
            for row in tournaments_data:
                win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
                
                # Determina status
                status = "finished"
                now = datetime.now(timezone.utc)
                
                if row.ends_on:
                    ends_on_utc = row.ends_on.replace(tzinfo=timezone.utc) if row.ends_on.tzinfo is None else row.ends_on
                    if ends_on_utc > now:
                        status = "active"
                
                tournaments.append({
                    "id": str(row.id),
                    "name": row.name,
                    "logo": row.logo,
                    "organizer": row.organizer,
                    "matches_played": row.matches_played,
                    "wins": row.wins,
                    "losses": row.total_matches - row.wins,
                    "win_rate": round(win_rate, 1),
                    "status": status,
                    "starts_on": row.starts_on.isoformat() if row.starts_on else None,
                    "ends_on": row.ends_on.isoformat() if row.ends_on else None
                })
        
        # 7. Estatísticas de mapas
        map_stats = await crud.get_team_map_stats(db, team_id)
        
        # Formata estatísticas de mapas para o retorno
        map_statistics = {
            "overall": map_stats["overall_stats"],
            "by_map": []
        }
        
        # Resume as estatísticas por mapa
        for map_stat in map_stats.get("maps", []):
            map_statistics["by_map"].append({
                "map": map_stat["map_name"],
                "matches": map_stat["matches_played"],
                "winrate": map_stat["winrate_percent"],
                "round_winrate": map_stat["rounds"]["round_winrate_percent"],
                "playrate": map_stat["playrate_percent"],
                "last_played": map_stat["dates"]["last_played"],
                "performance": {
                    "wins": map_stat["wins"],
                    "losses": map_stat["losses"],
                    "draws": map_stat["draws"]
                },
                "rounds": {
                    "total_won": map_stat["rounds"]["total_won"],
                    "total_lost": map_stat["rounds"]["total_lost"],
                    "avg_won": map_stat["rounds"]["avg_won_per_match"],
                    "avg_lost": map_stat["rounds"]["avg_lost_per_match"]
                }
            })
        
        # 8. Monta lista de partidas com proteção contra None
        matches_list = []
        for match in recent_matches:
            try:
                match_dict = {
                    "id": str(match.id) if match.id else None,
                    "date": match.date.isoformat() if match.date else None,
                    "map": match.map,
                    "url": getattr(match, 'url', None),  # Usa getattr para evitar AttributeError
                }
                
                # Tournament info
                if match.tournament:
                    match_dict["tournament"] = {
                        "id": str(match.tournament.id) if match.tournament.id else None,
                        "name": match.tournament.name if hasattr(match.tournament, 'name') else None,
                        "logo": match.tournament.logo if hasattr(match.tournament, 'logo') else None
                    }
                else:
                    match_dict["tournament"] = None
                
                # Team A info
                if match.tmi_a and match.tmi_a.team:
                    match_dict["team_a"] = {
                        "id": match.tmi_a.team.id,
                        "name": match.tmi_a.team.name,
                        "tag": match.tmi_a.team.tag,
                        "score": match.tmi_a.score
                    }
                else:
                    match_dict["team_a"] = None
                
                # Team B info
                if match.tmi_b and match.tmi_b.team:
                    match_dict["team_b"] = {
                        "id": match.tmi_b.team.id,
                        "name": match.tmi_b.team.name,
                        "tag": match.tmi_b.team.tag,
                        "score": match.tmi_b.score
                    }
                else:
                    match_dict["team_b"] = None
                
                # Adiciona picks_bans se existir
                if hasattr(match, 'picks_bans'):
                    match_dict["picks_bans"] = match.picks_bans
                else:
                    match_dict["picks_bans"] = None
                
                matches_list.append(match_dict)
                
            except Exception as e:
                logger.warning(f"Erro ao processar partida {match.id if match else 'Unknown'}: {e}")
                continue
        
        # 9. Monta resposta completa
        return {
            "team": {
                "id": team.id,
                "name": team.name,
                "tag": team.tag,
                "slug": team.slug,
                "logo": team.logo,
                "university": team.university,
                "university_tag": team.university_tag,
                "estado": team.estado,
                "social_media": {
                    "instagram": team.instagram,
                    "twitch": team.twitch,
                }
            },
            "roster": {
                "count": len(players),
                "players": players
            },
            "ranking": {
                "current": current_ranking,
                "history": ranking_history,
                "available": RANKING_AVAILABLE
            },
            "statistics": stats,
            "map_statistics": map_statistics,
            "recent_matches": {
                "count": len(matches_list),
                "matches": matches_list
            },
            "tournaments": {
                "count": len(tournaments),
                "list": tournaments
            },
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado em get_team_complete_info: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro interno ao processar dados do time")


@app.get("/teams/{team_id}/map-stats", tags=["teams", "stats"])
async def get_team_map_statistics(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna estatísticas detalhadas de desempenho em mapas para uma equipe.
    
    Inclui:
    - Total de partidas jogadas por mapa
    - Vitórias, empates e derrotas em cada mapa
    - Taxa de vitória (winrate) por mapa
    - Rounds ganhos/perdidos por mapa
    - Taxa de vitória de rounds por mapa
    - Playrate (% de vezes que o mapa foi jogado)
    - Maiores vitórias e derrotas por mapa
    - Histórico recente em cada mapa
    """
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca as estatísticas
    stats = await crud.get_team_map_stats(db, team_id)
    
    # Adiciona informações do time
    stats["team"] = {
        "id": team.id,
        "name": team.name,
        "tag": team.tag,
        "university": team.university,
        "estado": team.estado
    }
    
    return stats


@app.get("/teams/{team_id}/map-comparison", tags=["teams", "stats"])
async def get_team_map_comparison(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna uma comparação visual das estatísticas em diferentes mapas.
    Útil para gráficos e análises comparativas.
    """
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca as estatísticas completas
    full_stats = await crud.get_team_map_stats(db, team_id)
    
    # Formata para comparação mais fácil
    comparison = {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag
        },
        "overall_winrate": full_stats["overall_stats"]["overall_winrate"],
        "maps_comparison": []
    }
    
    # Ordena mapas por winrate para facilitar visualização
    sorted_maps = sorted(
        full_stats["maps"], 
        key=lambda x: x["winrate_percent"], 
        reverse=True
    )
    
    for map_stat in sorted_maps:
        comparison["maps_comparison"].append({
            "map": map_stat["map_name"],
            "matches": map_stat["matches_played"],
            "winrate": map_stat["winrate_percent"],
            "round_winrate": map_stat["rounds"]["round_winrate_percent"],
            "playrate": map_stat["playrate_percent"],
            "performance": {
                "wins": map_stat["wins"],
                "losses": map_stat["losses"],
                "draws": map_stat["draws"]
            },
            "avg_score": {
                "team": map_stat["rounds"]["avg_won_per_match"],
                "opponent": map_stat["rounds"]["avg_lost_per_match"]
            },
            "rating": _calculate_map_rating(map_stat)
        })
    
    # Adiciona melhores e piores mapas
    if comparison["maps_comparison"]:
        comparison["best_maps"] = comparison["maps_comparison"][:3]
        comparison["worst_maps"] = comparison["maps_comparison"][-3:] if len(comparison["maps_comparison"]) > 3 else []
    
    return comparison


def _calculate_map_rating(map_stat):
    """
    Calcula uma pontuação geral do desempenho no mapa
    baseado em winrate, rounds e consistência
    """
    winrate_weight = 0.5
    round_winrate_weight = 0.3
    volume_weight = 0.2
    
    # Normaliza volume de partidas (até 20 partidas)
    volume_score = min(map_stat["matches_played"] / 20, 1) * 100
    
    rating = (
        map_stat["winrate_percent"] * winrate_weight +
        map_stat["rounds"]["round_winrate_percent"] * round_winrate_weight +
        volume_score * volume_weight
    )
    
    return round(rating, 2)

@app.get("/ranking/stats", tags=["ranking"])
async def get_ranking_stats(
    db: AsyncSession = Depends(get_db)
):
    """Retorna estatísticas sobre o ranking atual"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        # Busca ranking do cache ou calcula novo
        ranking_response = await get_ranking(db=db)
        
        # Verifica se o retorno tem a estrutura esperada
        if not isinstance(ranking_response, dict) or "ranking" not in ranking_response:
            logger.error(f"Resposta inesperada de get_ranking: {type(ranking_response)}")
            raise HTTPException(status_code=500, detail="Formato de resposta inválido do ranking")
        
        ranking_data = ranking_response["ranking"]
        
        # Verifica se ranking_data é uma lista
        if not isinstance(ranking_data, list):
            logger.error(f"ranking_data não é uma lista: {type(ranking_data)}")
            raise HTTPException(status_code=500, detail="Dados de ranking em formato inválido")
        
        if not ranking_data:
            raise HTTPException(status_code=404, detail="Nenhum dado de ranking disponível")
        
        # Calcula estatísticas
        notas = [item["nota_final"] for item in ranking_data if isinstance(item, dict) and "nota_final" in item]
        incertezas = [item["incerteza"] for item in ranking_data if isinstance(item, dict) and "incerteza" in item]
        games = [item["games_count"] for item in ranking_data if isinstance(item, dict) and "games_count" in item]
        
        if not notas:
            raise HTTPException(status_code=500, detail="Dados de ranking incompletos")
        
        # Distribuição por faixas
        faixas = {
            "top_10": sum(1 for n in notas if n >= 90),
            "80_89": sum(1 for n in notas if 80 <= n < 90),
            "70_79": sum(1 for n in notas if 70 <= n < 80),
            "60_69": sum(1 for n in notas if 60 <= n < 70),
            "50_59": sum(1 for n in notas if 50 <= n < 60),
            "below_50": sum(1 for n in notas if n < 50)
        }
        
        # Top 5 e Bottom 5
        top_5 = ranking_data[:5] if len(ranking_data) >= 5 else ranking_data
        bottom_5 = ranking_data[-5:] if len(ranking_data) > 5 else []
        
        # Cálculo de desvio padrão
        mean_nota = sum(notas) / len(notas)
        std_dev = (sum((x - mean_nota)**2 for x in notas) / len(notas))**0.5
        
        return {
            "total_teams": len(ranking_data),
            "stats": {
                "nota_final": {
                    "max": max(notas),
                    "min": min(notas),
                    "avg": mean_nota,
                    "std_dev": std_dev
                },
                "games_count": {
                    "max": max(games) if games else 0,
                    "min": min(games) if games else 0,
                    "avg": sum(games) / len(games) if games else 0
                },
                "incerteza": {
                    "max": max(incertezas) if incertezas else 0,
                    "min": min(incertezas) if incertezas else 0,
                    "mean": sum(incertezas) / len(incertezas) if incertezas else 0
                }
            },
            "distribution": faixas,
            "top_5": top_5,
            "bottom_5": bottom_5,
            "last_update": ranking_response.get("last_update", ""),
            "cached": ranking_response.get("cached", False)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")

@app.get("/teams/{team_id}/social-media", tags=["teams"])
async def get_team_social_media(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna apenas as redes sociais de um time"""
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    return {
        "team_id": team.id,
        "team_name": team.name,
        "estado": team.estado,  # NOVO CAMPO
        "social_media": {
            "instagram": team.instagram,
            "twitch": team.twitch
        }
    }

@app.patch("/teams/{team_id}/social-media", tags=["teams", "admin"])
async def update_team_social_media(
    team_id: int,
    instagram: str = Query(None, max_length=100),
    twitch: str = Query(None, max_length=100),
    admin_key: str = Query(..., description="Chave de administrador"),
    db: AsyncSession = Depends(get_db)
):
    """
    Atualiza as redes sociais de um time (endpoint protegido).
    Apenas campos fornecidos serão atualizados.
    """
    # Verifica chave de admin
    if admin_key != os.getenv("ADMIN_KEY", "valorant2024admin"):
        raise HTTPException(status_code=403, detail="Chave de administrador inválida")
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Prepara updates
    updates = {}
    if instagram is not None:
        updates["instagram"] = instagram
    if twitch is not None:
        updates["twitch"] = twitch
    
    if updates:
        # Executa update
        stmt = (
            update(Team)
            .where(Team.id == team_id)
            .values(**updates)
        )
        await db.execute(stmt)
        await db.commit()
    
    # Busca time atualizado
    updated_team = await crud.get_team(db, team_id)
    
    return {
        "success": True,
        "team_id": team_id,
        "updated_fields": list(updates.keys()),
        "social_media": {
            "instagram": updated_team.instagram,
            "twitch": updated_team.twitch,
        }
    }

# ════════════════════════════════ PLAYERS ════════════════════════════════

@app.get("/teams/players/summary", tags=["players"])
async def get_all_teams_players(db: AsyncSession = Depends(get_db)):
    """Retorna um resumo de todos os times e seus jogadores"""
    
    stmt = text("""
        SELECT 
            t.id,
            t.name,
            t.tag,
            t.university,
            COUNT(tp.id) as player_count,
            ARRAY_AGG(tp.player_nick ORDER BY tp.id) FILTER (WHERE tp.player_nick IS NOT NULL) as players
        FROM teams t
        LEFT JOIN team_players tp ON t.id = tp.team_id
        GROUP BY t.id, t.name, t.tag, t.university
        ORDER BY t.name
    """)
    
    result = await db.execute(stmt)
    
    teams_data = []
    for row in result:
        teams_data.append({
            "team_id": row.id,
            "team_name": row.name,
            "team_tag": row.tag,
            "university": row.university,
            "player_count": row.player_count or 0,
            "players": row.players or []
        })
    
    # Estatísticas
    total_teams = len(teams_data)
    teams_with_players = sum(1 for t in teams_data if t["player_count"] > 0)
    total_players = sum(t["player_count"] for t in teams_data)
    
    return {
        "statistics": {
            "total_teams": total_teams,
            "teams_with_players": teams_with_players,
            "teams_without_players": total_teams - teams_with_players,
            "total_players": total_players,
            "average_players_per_team": round(total_players / teams_with_players, 2) if teams_with_players > 0 else 0
        },
        "teams": teams_data
    }

@app.get("/players/search", tags=["players"])
async def search_players(
    q: str = Query(..., min_length=2, description="Nome do jogador para buscar"),
    db: AsyncSession = Depends(get_db)
):
    """Busca jogadores por nome"""
    
    stmt = text("""
        SELECT DISTINCT
            tp.player_nick,
            t.id as team_id,
            t.name as team_name,
            t.tag as team_tag,
            t.university as university
        FROM team_players tp
        JOIN teams t ON tp.team_id = t.id
        WHERE LOWER(tp.player_nick) LIKE LOWER(:search_term)
        ORDER BY tp.player_nick, t.name
        LIMIT 50
    """)
    
    result = await db.execute(stmt, {"search_term": f"%{q}%"})
    
    players = []
    for row in result:
        players.append({
            "player_nick": row.player_nick,
            "team_id": row.team_id,
            "team_name": row.team_name,
            "team_tag": row.team_tag,
            "university": row.university
        })
    
    return {
        "query": q,
        "count": len(players),
        "players": players
    }

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

@app.get("/tournaments", response_model=List[schemas.Tournament], tags=["tournaments"])
async def list_tournaments(db: AsyncSession = Depends(get_db)):
    """Lista todos os torneios ordenados por data de início"""
    return await crud.list_tournaments(db)

@app.get("/tournaments/{tournament_id}", response_model=schemas.Tournament, tags=["tournaments"])
async def get_tournament(tournament_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Retorna detalhes de um torneio específico"""
    tournament = await crud.get_tournament(db, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Torneio não encontrado")
    return tournament

@app.get("/tournaments/{tournament_id}/matches", response_model=List[schemas.Match], tags=["tournaments"])
async def get_tournament_matches(
    tournament_id: uuid.UUID,
    db: AsyncSession = Depends(get_db)
):
    """Retorna todas as partidas de um torneio"""
    tournament = await crud.get_tournament(db, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Torneio não encontrado")
    
    return await crud.get_tournament_matches(db, tournament_id)

# ════════════════════════════════ MATCHES ════════════════════════════════

@app.get("/matches", response_model=List[schemas.Match], tags=["matches"])
async def list_matches(
    limit: int = Query(20, ge=1, le=100, description="Número de partidas a retornar"),
    db: AsyncSession = Depends(get_db),
):
    """Retorna as partidas mais recentes com informações completas"""
    return await crud.list_matches(db, limit=limit)

@app.get("/matches/{match_id}", response_model=schemas.Match, tags=["matches"])
async def get_match(match_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Retorna detalhes de uma partida específica"""
    match = await crud.get_match(db, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Partida não encontrada")
    return match

# ════════════════════════════════ STATS ════════════════════════════════

@app.get("/stats/maps", tags=["stats"])
async def get_maps_stats(db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas de mapas jogados"""
    return await crud.get_maps_played(db)

@app.get("/stats/summary", tags=["stats"])
async def get_general_stats(db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas gerais do sistema"""
    try:
        # Contagens básicas
        teams_count = await db.scalar(select(func.count(Team.id)))
        matches_count = await db.scalar(select(func.count(Match.id)))
        tournaments_count = await db.scalar(select(func.count(Tournament.id)))
        players_count = await db.scalar(select(func.count(TeamPlayer.id)))
        
        # Times com mais vitórias
        stmt = text("""
            SELECT 
                t.id,
                t.name,
                t.tag,
                COUNT(CASE WHEN tmi.score > opponent.score THEN 1 END) as wins,
                COUNT(*) as total_matches
            FROM teams t
            JOIN team_match_info tmi ON t.id = tmi.team_id
            JOIN matches m ON (m.team_match_info_a = tmi.id OR m.team_match_info_b = tmi.id)
            JOIN team_match_info opponent ON 
                (CASE 
                    WHEN m.team_match_info_a = tmi.id THEN m.team_match_info_b = opponent.id
                    ELSE m.team_match_info_a = opponent.id
                END)
            GROUP BY t.id, t.name, t.tag
            HAVING COUNT(*) > 5
            ORDER BY wins DESC
            LIMIT 10
        """)
        
        top_teams_result = await db.execute(stmt)
        top_teams = []
        for row in top_teams_result:
            win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
            top_teams.append({
                "id": row.id,
                "name": row.name,
                "tag": row.tag,
                "wins": row.wins,
                "total_matches": row.total_matches,
                "win_rate": round(win_rate, 1)
            })
        
        # Última atualização
        last_match_stmt = select(func.max(Match.date))
        last_match_date = await db.scalar(last_match_stmt)
        
        return {
            "counts": {
                "teams": teams_count,
                "matches": matches_count,
                "tournaments": tournaments_count,
                "players": players_count
            },
            "top_teams": top_teams,
            "last_update": last_match_date.isoformat() if last_match_date else None
        }
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas gerais: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao calcular estatísticas")

# ════════════════════════════════ RANKING ════════════════════════════════

@app.get("/ranking", tags=["ranking"])
async def get_ranking(
    limit: int = Query(None, ge=1, le=100),
    force_refresh: bool = Query(False, description="Força recálculo do ranking"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna o ranking atual dos times.
    Por padrão usa cache de 1 hora. Use force_refresh=true para forçar recálculo.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(
            status_code=503, 
            detail="Sistema de ranking não disponível. " +
                   "Instale as dependências científicas."
        )
    
    now = datetime.now(timezone.utc)
    
    # Verifica cache
    if (not force_refresh and 
        ranking_cache["data"] is not None and 
        ranking_cache["timestamp"] is not None and
        now - ranking_cache["timestamp"] < ranking_cache["ttl"]):
        
        data = ranking_cache["data"]
        if limit:
            data = data[:limit]
        
        return {
            "ranking": data,
            "total": len(ranking_cache["data"]),
            "limit": limit,
            "cached": True,
            "cache_age_seconds": int((now - ranking_cache["timestamp"]).total_seconds()),
            "last_update": ranking_cache["timestamp"].isoformat()
        }
    
    # Calcula novo ranking
    try:
        ranking_data = await calculate_ranking(db)
        
        # Atualiza cache
        ranking_cache["data"] = ranking_data
        ranking_cache["timestamp"] = now
        
        if limit:
            ranking_data = ranking_data[:limit]
        
        return {
            "ranking": ranking_data,
            "total": len(ranking_cache["data"]),
            "limit": limit,
            "cached": False,
            "last_update": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular ranking: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao calcular ranking: {str(e)}")

@app.get("/ranking/snapshots", tags=["ranking"])
async def list_snapshots(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Lista todos os snapshots disponíveis"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    snapshots = result.scalars().all()
    
    return {
        "snapshots": [
            {
                "id": s.id,
                "created_at": s.created_at.isoformat(),
                "total_teams": s.total_teams,
                "total_matches": s.total_matches,
                "metadata": s.snapshot_metadata
            }
            for s in snapshots
        ],
        "count": len(snapshots)
    }

@app.post("/ranking/refresh", tags=["ranking"])
async def refresh_ranking(
    db: AsyncSession = Depends(get_db),
    secret_key: str = Query(..., description="Chave para autorizar refresh")
):
    """Força o recálculo do ranking (endpoint protegido)"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    if secret_key != os.getenv("RANKING_REFRESH_KEY", "valorant2024ranking"):
        raise HTTPException(status_code=403, detail="Chave inválida")
    
    # Limpa cache
    ranking_cache["data"] = None
    ranking_cache["timestamp"] = None
    
    # Recalcula
    result = await get_ranking(db=db, force_refresh=True)
    
    return {
        "success": True,
        "message": "Ranking recalculado com sucesso",
        "total_teams": result["total"],
        "timestamp": result["last_update"]
    }

@app.get("/ranking/team/{team_id}/history", tags=["ranking"])
async def get_team_ranking_history(
    team_id: int,
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Retorna o histórico de posições de um time ao longo dos snapshots"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica se o time existe
    stmt = select(Team).where(Team.id == team_id)
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail=f"Time {team_id} não encontrado")
    
    # Busca o histórico
    stmt = (
        select(
            RankingHistory,
            RankingSnapshot.created_at,
            RankingSnapshot.total_teams
        )
        .join(RankingSnapshot, RankingHistory.snapshot_id == RankingSnapshot.id)
        .where(RankingHistory.team_id == team_id)
        .order_by(RankingSnapshot.created_at.desc())
        .limit(limit)
    )
    
    result = await db.execute(stmt)
    history_entries = []
    
    for entry, created_at, total_teams in result:
        history_entries.append({
            "date": created_at.isoformat(),
            "position": entry.position,
            "nota_final": float(entry.nota_final),
            "ci_lower": float(entry.ci_lower),
            "ci_upper": float(entry.ci_upper),
            "games_count": entry.games_count,
            "total_teams": total_teams,
            "scores": {
                "colley": float(entry.score_colley) if entry.score_colley else None,
                "massey": float(entry.score_massey) if entry.score_massey else None,
                "elo": float(entry.score_elo_final) if entry.score_elo_final else None,
                "elo_mov": float(entry.score_elo_mov) if entry.score_elo_mov else None,
                "trueskill": float(entry.score_trueskill) if entry.score_trueskill else None,
                "pagerank": float(entry.score_pagerank) if entry.score_pagerank else None,
                "bradley_terry": float(entry.score_bradley_terry) if entry.score_bradley_terry else None,
                "pca": float(entry.score_pca) if entry.score_pca else None,
                "sos": float(entry.score_sos) if entry.score_sos else None,
                "consistency": float(entry.score_consistency) if entry.score_consistency else None,
                "borda": entry.score_borda if entry.score_borda else None,
                "integrado": float(entry.score_integrado) if entry.score_integrado else None
            }
        })
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "university": team.university
        },
        "history": history_entries,
        "count": len(history_entries)
    }

@app.post("/ranking/snapshot", tags=["ranking", "admin"])
async def create_ranking_snapshot(
    db: AsyncSession = Depends(get_db),
    admin_key: str = Query(..., description="Chave de administrador")
):
    """
    Cria um novo snapshot do ranking atual (endpoint protegido).
    Use com cuidado - isso salva permanentemente o estado atual do ranking.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica chave de admin
    if admin_key != os.getenv("ADMIN_KEY", "valorant2024admin"):
        raise HTTPException(status_code=403, detail="Chave de administrador inválida")
    
    try:
        # Salva snapshot
        snapshot_id = await save_ranking_snapshot(db)
        
        if snapshot_id:
            await db.commit()
            
            # Busca informações do snapshot criado
            stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
            result = await db.execute(stmt)
            snapshot = result.scalar_one()
            
            return {
                "success": True,
                "snapshot_id": snapshot.id,
                "created_at": snapshot.created_at.isoformat(),
                "total_teams": snapshot.total_teams,
                "total_matches": snapshot.total_matches,
                "metadata": snapshot.snapshot_metadata
            }
        else:
            raise HTTPException(
                status_code=500, 
                detail="Erro ao criar snapshot - nenhum dado de ranking disponível"
            )
            
    except Exception as e:
        await db.rollback()
        logger.error(f"Erro ao criar snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar snapshot: {str(e)}")

# ════════════════════════════════ DEBUG ════════════════════════════════

@app.get("/debug/team/{team_id}/data", tags=["debug"])
async def debug_team_data(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Endpoint de debug para verificar dados de um time.
    Útil para diagnosticar problemas com estatísticas ou partidas.
    """
    
    response = {
        "team_id": team_id,
        "team_exists": False,
        "team_match_info_count": 0,
        "match_count": 0,
        "errors": []
    }
    
    try:
        # 1. Verifica se o time existe
        team = await crud.get_team(db, team_id)
        if team:
            response["team_exists"] = True
            response["team_info"] = {
                "id": team.id,
                "name": team.name,
                "tag": team.tag,
                "university": team.university
            }
        else:
            response["errors"].append(f"Time {team_id} não encontrado")
            return response
        
        # 2. Conta TeamMatchInfo do time
        tmi_count_stmt = text("""
            SELECT COUNT(*) 
            FROM team_match_info 
            WHERE team_id = :team_id
        """)
        tmi_count = await db.scalar(tmi_count_stmt, {"team_id": team_id})
        response["team_match_info_count"] = tmi_count
        
        # 3. Busca amostra de TeamMatchInfo
        sample_tmi_stmt = text("""
            SELECT id, score, agent_1, agent_2, agent_3, agent_4, agent_5
            FROM team_match_info 
            WHERE team_id = :team_id
            LIMIT 5
        """)
        result = await db.execute(sample_tmi_stmt, {"team_id": team_id})
        sample_tmis = []
        for row in result:
            sample_tmis.append({
                "id": str(row.id),
                "score": row.score,
                "agents": [row.agent_1, row.agent_2, row.agent_3, row.agent_4, row.agent_5]
            })
        response["sample_tmi"] = sample_tmis
        
        # 4. Conta partidas onde o time aparece
        match_count_stmt = text("""
            SELECT COUNT(DISTINCT m.id)
            FROM matches m
            JOIN team_match_info tmi ON (
                m.team_match_info_a = tmi.id OR 
                m.team_match_info_b = tmi.id
            )
            WHERE tmi.team_id = :team_id
        """)
        match_count = await db.scalar(match_count_stmt, {"team_id": team_id})
        response["match_count"] = match_count
        
        # 5. Tenta buscar uma partida completa
        try:
            matches = await crud.get_team_matches(db, team_id, limit=1)
            response["get_team_matches_success"] = True
            response["matches_returned"] = len(matches)
        except Exception as e:
            response["get_team_matches_success"] = False
            response["get_team_matches_error"] = str(e)
            response["errors"].append(f"Erro em get_team_matches: {str(e)}")
            
            # Tenta query mais simples
            try:
                simple_query = text("""
                    SELECT m.id, m.map, m.date
                    FROM matches m
                    JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
                    JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
                    WHERE tmi_a.team_id = :team_id OR tmi_b.team_id = :team_id
                    LIMIT 1
                """)
                simple_result = await db.execute(simple_query, {"team_id": team_id})
                simple_match = simple_result.first()
                if simple_match:
                    response["simple_query_success"] = True
                    response["sample_match"] = {
                        "id": str(simple_match.id),
                        "map": simple_match.map,
                        "date": simple_match.date.isoformat() if simple_match.date else None
                    }
                else:
                    response["simple_query_success"] = False
                    response["errors"].append("Nenhuma partida encontrada na query simples")
            except Exception as e2:
                response["simple_query_error"] = str(e2)
                response["errors"].append(f"Erro na query simples: {str(e2)}")
        
        return response
        
    except Exception as e:
        response["errors"].append(f"Erro geral: {str(e)}")
        import traceback
        response["traceback"] = traceback.format_exc()
        return response

# Correção da função get_api_info (substituir a partir da linha ~1195)

@app.get("/info", tags=["root"])
async def get_api_info(db: AsyncSession = Depends(get_db)):
    """
    Retorna informações sobre a API e o estado do sistema.
    """
    
    # Verifica última snapshot de ranking
    last_snapshot = None
    if RANKING_AVAILABLE:
        try:
            stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
            result = await db.execute(stmt)
            latest = result.scalar_one_or_none()
            
            if latest:
                # Calcula tempo desde último snapshot
                now = datetime.now(timezone.utc)
                time_since = now - latest.created_at
                hours_ago = time_since.total_seconds() / 3600
                days_ago = hours_ago / 24
                
                # Conta times no último snapshot
                count_stmt = select(func.count(RankingHistory.id)).where(
                    RankingHistory.snapshot_id == latest.id
                )
                count = await db.scalar(count_stmt)
                
                # Estatísticas do snapshot
                stats_stmt = select(
                    func.avg(RankingHistory.nota_final),
                    func.max(RankingHistory.nota_final),
                    func.min(RankingHistory.nota_final)
                ).where(RankingHistory.snapshot_id == latest.id)
                
                stats_result = await db.execute(stats_stmt)
                avg_nota, max_nota, min_nota = stats_result.first()
                
                last_snapshot = {
                    "id": latest.id,
                    "created_at": latest.created_at.isoformat(),
                    "total_teams": latest.total_teams,
                    "total_matches": latest.total_matches,
                    "metadata": latest.snapshot_metadata,
                    "stats": {
                        "teams_ranked": count or 0,
                        "avg_nota": float(avg_nota) if avg_nota else 0,
                        "max_nota": float(max_nota) if max_nota else 0,
                        "min_nota": float(min_nota) if min_nota else 0
                    },
                    "time_since": {
                        "hours": round(hours_ago, 1),
                        "days": round(days_ago, 1),
                        "human_readable": f"{round(days_ago)} dias atrás" if days_ago >= 1 else f"{round(hours_ago)} horas atrás"
                    }
                }
        except Exception as e:
            logger.warning(f"Erro ao buscar último snapshot: {e}")
    
    return {
        "api": {
            "name": "Valorant Universitário API",
            "version": "1.0.0",
            "docs_url": "/docs"
        },
        "features": {
            "ranking_available": RANKING_AVAILABLE,
            "cache_ttl_hours": ranking_cache["ttl"].total_seconds() / 3600,
            "social_media_support": True
        },
        "last_snapshot": last_snapshot,
        "endpoints": {
            "teams": "/teams",
            "tournaments": "/tournaments", 
            "matches": "/matches",
            "ranking": "/ranking" if RANKING_AVAILABLE else None,
            "stats": "/stats/summary"
        }
    }

# ════════════════════════════════ RANKING FUNCTIONS ════════════════════════════════

async def calculate_ranking(db: AsyncSession, include_variation: bool = True) -> List[Dict[str, Any]]:
    """Função principal para calcular o ranking"""
    try:
        # Busca todos os times
        teams_result = await db.execute(select(Team))
        teams = teams_result.scalars().all()
        logger.info(f"🔄 Total de times no banco: {len(teams)}")
        
        # Busca TODAS as partidas sem distinct() para debugar
        matches_stmt = (
            select(Match)
            .options(
                selectinload(Match.tournament),
                selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
                selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
            )
            .order_by(Match.date)
        )
        
        matches_result = await db.execute(matches_stmt)
        all_matches = list(matches_result.scalars())
        logger.info(f"📊 Total de partidas brutas no banco: {len(all_matches)}")
        
        # Detecta duplicatas para debug
        match_keys = set()
        unique_matches = []
        duplicates = 0
        
        for match in all_matches:
            if not match.tmi_a or not match.tmi_b or not match.tmi_a.team or not match.tmi_b.team:
                continue
                
            # Cria chave única
            key = tuple(sorted([
                match.tmi_a.team.name.strip(),
                match.tmi_b.team.name.strip()
            ]) + [
                match.date.strftime("%Y-%m-%d %H:%M"),
                match.map
            ])
            
            if key in match_keys:
                duplicates += 1
            else:
                match_keys.add(key)
                unique_matches.append(match)
        
        logger.info(f"⚠️ Duplicatas detectadas: {duplicates}")
        logger.info(f"✔️ Partidas únicas: {len(unique_matches)}")
        
        if len(unique_matches) == 0:
            logger.warning("Nenhuma partida válida encontrada")
            return []
        
        # Calcula o ranking com partidas únicas
        calculator = RankingCalculator(teams, unique_matches)
        ranking_df = calculator.calculate_final_ranking()
        
        # Ordena por nota final e reseta índice
        ranking_df = ranking_df.sort_values('NOTA_FINAL', ascending=False).reset_index(drop=True)
        
        # Busca último snapshot para calcular variação
        previous_positions = {}
        if include_variation:
            try:
                from models import RankingSnapshot, RankingHistory
                
                # Busca o último snapshot
                snapshot_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).offset(1).limit(1)
                snapshot_result = await db.execute(snapshot_stmt)
                last_snapshot = snapshot_result.scalar_one_or_none()
                
                if last_snapshot:
                    # Busca as posições do último snapshot
                    history_stmt = (
                        select(RankingHistory)
                        .where(RankingHistory.snapshot_id == last_snapshot.id)
                    )
                    history_result = await db.execute(history_stmt)
                    
                    for history_entry in history_result.scalars():
                        previous_positions[history_entry.team_id] = history_entry.position
                    
                    logger.info(f"📊 Comparando com snapshot #{last_snapshot.id} de {last_snapshot.created_at}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao buscar snapshot anterior: {e}")
        
        # Converte para formato da API
        result = []
        for idx, row in ranking_df.iterrows():
            # idx agora é garantidamente um inteiro
            position = int(idx) + 1
            
            # Calcula variação e verifica se é novo
            variacao = None
            is_new = False
            
            if include_variation and pd.notna(row.team_id):
                team_id_int = int(row.team_id)
                if team_id_int in previous_positions:
                    posicao_anterior = previous_positions[team_id_int]
                    variacao = posicao_anterior - position  # Positivo = subiu, Negativo = desceu
                else:
                    # Time não estava no ranking anterior - é novo!
                    is_new = True
            
            result.append({
                "posicao": position,
                "team_id": int(row.team_id) if pd.notna(row.team_id) else None,
                "team": row.team,
                "tag": row.tag,
                "university": row.university,
                "nota_final": float(row.NOTA_FINAL),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": int(row.games_count),
                "variacao": variacao,
                "is_new": is_new,
                "scores": {
                    "colley": float(row.r_colley),
                    "massey": float(row.r_massey),
                    "elo": float(row.r_elo_final),
                    "elo_mov": float(row.r_elo_mov),
                    "trueskill": float(row.ts_score),
                    "pagerank": float(row.r_pagerank),
                    "bradley_terry": float(row.r_bt_pois),
                    "pca": float(row.pca_score),
                    "sos": float(row.sos_score),
                    "consistency": float(row.consistency),
                    "borda": int(row.borda_score),
                    "integrado": float(row.rating_integrado)
                },
                "anomaly": {
                    "is_anomaly": bool(row.is_anomaly),
                    "score": float(row.anomaly_score)
                }
            })
        
        logger.info(f"🏆 Ranking calculado com sucesso para {len(result)} times")
        return result
        
    except Exception as e:
        logger.error(f"❌ Erro ao calcular ranking: {str(e)}", exc_info=True)
        raise