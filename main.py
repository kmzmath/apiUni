import os
from pathlib import Path as PathLib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, Depends, HTTPException, Query, Path
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

def _f(v): return float(v) if v is not None else None


# ───── SERIALIZAÇÃO PADRÃO DE RANKING ─────
def _row_to_ranking_item(row) -> dict:
    """Converte uma linha de SELECT (ranking_history JOIN teams)
    para o mesmo formato usado em /ranking."""
    return {
        "posicao":       row.position,
        "team_id":       row.team_id,
        "team":          row.name,
        "tag":           row.tag,
        "university":    row.university,
        "nota_final":    float(row.nota_final),
        "ci_lower":      float(row.ci_lower),
        "ci_upper":      float(row.ci_upper),
        "incerteza":     float(row.incerteza),
        "games_count":   row.games_count,
        "variacao":      None,
        "variacao_nota": None,
        "is_new":        False,
        "scores": {
            "colley":        _f(row.score_colley),
            "massey":        _f(row.score_massey),
            "elo":           _f(row.score_elo_final),
            "elo_mov":       _f(row.score_elo_mov),
            "trueskill":     _f(row.score_trueskill),
            "pagerank":      _f(row.score_pagerank),
            "bradley_terry": _f(row.score_bradley_terry),
            "pca":           _f(row.score_pca),
            "sos":           _f(row.score_sos),
            "consistency":   _f(row.score_consistency),
            "borda":         row.score_borda,
            "integrado":     _f(row.score_integrado),
        },
        "anomaly": {
            "is_anomaly": bool(row.is_anomaly),
            "score": _f(row.anomaly_score),
        },
    }


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
    # Faz o join com a tabela estados para trazer as informações completas
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.slug == slug)
    )
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Prepara as informações do estado
    estado_info = None
    if team.estado_obj:
        estado_info = {
            "id": team.estado_obj.id,
            "sigla": team.estado_obj.sigla,
            "nome": team.estado_obj.nome,
            "icone": team.estado_obj.icone,
            "regiao": team.estado_obj.regiao
        }
    
    return {
        "id": team.id,
        "name": team.name,
        "logo": team.logo,
        "tag": team.tag,
        "slug": team.slug,
        "university": team.university,
        "university_tag": team.university_tag,
        "estado": team.estado,  # Campo string original (mantido para compatibilidade)
        "estado_info": estado_info,  # NOVO: Informações completas do estado
        "instagram": team.instagram,
        "twitch": team.twitch
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
        
        # Top 5 e Bottom 5 - CORREÇÃO AQUI
        # Garante que estamos trabalhando com listas e índices inteiros
        ranking_list = list(ranking_data)  # Garante que é uma lista
        top_5 = ranking_list[:5] if len(ranking_list) >= 5 else ranking_list
        bottom_5 = ranking_list[-5:] if len(ranking_list) > 5 else []
        
        # Cálculo de desvio padrão
        mean_nota = sum(notas) / len(notas)
        std_dev = (sum((x - mean_nota)**2 for x in notas) / len(notas))**0.5
        
        # Converte items para o formato esperado se necessário
        def ensure_ranking_item_format(item):
            """Garante que o item tem o formato correto"""
            if isinstance(item, dict):
                return item
            elif hasattr(item, 'model_dump'):
                return item.model_dump()
            else:
                return item
        
        top_5_formatted = [ensure_ranking_item_format(item) for item in top_5]
        bottom_5_formatted = [ensure_ranking_item_format(item) for item in bottom_5]
        
        return {
            "total_teams": len(ranking_data),
            "stats": {
                "nota_final": {
                    "max": round(max(notas), 2),
                    "min": round(min(notas), 2),
                    "avg": round(mean_nota, 2),
                    "std_dev": round(std_dev, 2)
                },
                "games_count": {
                    "max": max(games) if games else 0,
                    "min": min(games) if games else 0,
                    "avg": round(sum(games) / len(games), 1) if games else 0
                },
                "incerteza": {
                    "max": round(max(incertezas), 2) if incertezas else 0,
                    "min": round(min(incertezas), 2) if incertezas else 0,
                    "mean": round(sum(incertezas) / len(incertezas), 2) if incertezas else 0
                }
            },
            "distribution": faixas,
            "top_5": top_5_formatted,
            "bottom_5": bottom_5_formatted,
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
    limit: Optional[int] = Query(None, ge=1, le=100),
    force_refresh: bool = Query(False, description="Força recálculo do ranking"),
    db: AsyncSession = Depends(get_db),
):
    """
    Retorna o ranking atual dos times.
    Por padrão usa cache de 1 hora.  Use ?force_refresh=true para recalcular.
    """

    # ── 1. Sanitiza limit ───────────────────────────────────────────────
    try:
        limit = int(limit) if limit is not None else None
    except (TypeError, ValueError):
        limit = None  # evita float ou string virarem índice

    if not RANKING_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Sistema de ranking não disponível. Instale dependências científicas.",
        )

    now = datetime.now(timezone.utc)

    # ── 2. Usa cache se válido ──────────────────────────────────────────
    if (
        not force_refresh
        and ranking_cache["data"] is not None
        and ranking_cache["timestamp"] is not None
        and now - ranking_cache["timestamp"] < ranking_cache["ttl"]
    ):
        data = ranking_cache["data"]
        if limit is not None:
            data = data[: limit]
        return {
            "ranking": data,
            "total": len(ranking_cache["data"]),
            "limit": limit,
            "cached": True,
            "cache_age_seconds": int((now - ranking_cache["timestamp"]).total_seconds()),
            "last_update": ranking_cache["timestamp"].isoformat(),
        }

    # ── 3. Recalcula ranking ────────────────────────────────────────────
    try:
        ranking_data = await calculate_ranking(db)

        # Atualiza cache
        ranking_cache["data"] = ranking_data
        ranking_cache["timestamp"] = now

        if limit is not None:
            ranking_data = ranking_data[: limit]

        return {
            "ranking": ranking_data,
            "total": len(ranking_cache["data"]),
            "limit": limit,
            "cached": False,
            "last_update": now.isoformat(),
        }

    except Exception as e:
        logger.error(f"Erro ao calcular ranking: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao calcular ranking: {e}")

@app.get("/ranking/snapshots", tags=["ranking"])
async def list_snapshots(
    limit: int = Query(20, ge=1, le=100),
    include_full_data: bool = Query(True, description="Incluir dados completos do ranking de cada snapshot"),
    db: AsyncSession = Depends(get_db)
):
    """
    Lista todos os snapshots disponíveis com dados completos.
    
    Por padrão, retorna os dados completos de cada snapshot incluindo:
    - Metadados do snapshot
    - Ranking completo com todas as equipes
    - Estatísticas do snapshot
    
    Use include_full_data=false para retornar apenas metadados (comportamento antigo).
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Busca os snapshots
    stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    snapshots = result.scalars().all()
    
    snapshots_data = []
    
    for snapshot in snapshots:
        snapshot_info = {
            "id": snapshot.id,
            "created_at": snapshot.created_at.isoformat(),
            "total_teams": snapshot.total_teams,
            "total_matches": snapshot.total_matches,
            "metadata": snapshot.snapshot_metadata
        }
        
        if include_full_data:
            # Busca dados completos do ranking para este snapshot
            history_stmt = text("""
                SELECT 
                    rh.*,
                    t.name,
                    t.tag,
                    t.university
                FROM ranking_history rh
                JOIN teams t ON rh.team_id = t.id
                WHERE rh.snapshot_id = :snapshot_id
                ORDER BY rh.position
            """)
            
            history_result = await db.execute(history_stmt, {"snapshot_id": snapshot.id})
            
            ranking_data = [_row_to_ranking_item(row) for row in history_result]
            
            # Calcula estatísticas
            if ranking_data:
                snapshot_info["ranking"] = ranking_data
                snapshot_info["statistics"] = {
                    "teams_count": len(ranking_data),
                    "avg_nota":    round(sum(r["nota_final"] for r in ranking_data) / len(ranking_data), 2),
                    "max_nota":    max(r["nota_final"] for r in ranking_data),
                    "min_nota":    min(r["nota_final"] for r in ranking_data),
                }
            else:
                snapshot_info["ranking"] = []
                snapshot_info["statistics"] = {
                    "teams_count": 0,
                    "avg_nota": 0,
                    "max_nota": 0,
                    "min_nota": 0,
                }
        
        snapshots_data.append(snapshot_info)
    
    return {
        "data": snapshots_data,
        "count": len(snapshots),
        "full_data_included": include_full_data
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

@app.get("/ranking/snapshots/compare", tags=["ranking"])
async def compare_ranking_snapshots(
    snapshot_1: int = Query(..., description="ID do primeiro snapshot"),
    snapshot_2: int = Query(..., description="ID do segundo snapshot"),
    db: AsyncSession = Depends(get_db)
):
    """
    Compara dois snapshots específicos do ranking e mostra as variações
    de posições e notas entre eles.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        from ranking_history import compare_snapshots
        
        comparison = await compare_snapshots(db, snapshot_1, snapshot_2)
        return comparison
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Erro ao comparar snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao comparar snapshots: {str(e)}")

# Adicione este endpoint na seção de RANKING do main.py

@app.delete("/ranking/snapshots/{snapshot_id}", tags=["ranking", "admin"])
async def delete_ranking_snapshot(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
    admin_key: str = Query(..., description="Chave de administrador")
):
    """
    Exclui um snapshot específico e todos os dados históricos associados (endpoint protegido).
    
    ⚠️ ATENÇÃO: Esta ação é IRREVERSÍVEL!
    - Remove o snapshot da tabela ranking_snapshots
    - Remove TODOS os registros associados da tabela ranking_history
    - Pode afetar o cálculo de variações no ranking se excluir snapshots recentes
    
    Recomendações:
    - Mantenha pelo menos os últimos 3-5 snapshots para histórico
    - Evite excluir o snapshot mais recente
    - Faça backup do banco antes de exclusões em massa
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica chave de admin
    if admin_key != os.getenv("ADMIN_KEY", "valorant2024admin"):
        raise HTTPException(status_code=403, detail="Chave de administrador inválida")
    
    try:
        # Verifica se o snapshot existe
        snapshot_stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
        snapshot_result = await db.execute(snapshot_stmt)
        snapshot = snapshot_result.scalar_one_or_none()
        
        if not snapshot:
            raise HTTPException(status_code=404, detail=f"Snapshot #{snapshot_id} não encontrado")
        
        # Conta quantos registros serão excluídos
        count_stmt = select(func.count(RankingHistory.id)).where(RankingHistory.snapshot_id == snapshot_id)
        history_count = await db.scalar(count_stmt) or 0
        
        # Verifica se é o único snapshot
        total_snapshots_stmt = select(func.count(RankingSnapshot.id))
        total_snapshots = await db.scalar(total_snapshots_stmt) or 0
        
        if total_snapshots <= 1:
            raise HTTPException(
                status_code=400, 
                detail="Não é possível excluir o único snapshot existente. Capture um novo antes de excluir este."
            )
        
        # Verifica se é o snapshot mais recente
        latest_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
        latest_result = await db.execute(latest_stmt)
        latest_snapshot = latest_result.scalar_one_or_none()
        
        is_latest = latest_snapshot and latest_snapshot.id == snapshot_id
        
        # Exclui registros do histórico (cascade deve cuidar disso, mas fazemos explicitamente)
        delete_history_stmt = delete(RankingHistory).where(RankingHistory.snapshot_id == snapshot_id)
        await db.execute(delete_history_stmt)
        
        # Exclui o snapshot
        delete_snapshot_stmt = delete(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
        await db.execute(delete_snapshot_stmt)
        
        await db.commit()
        
        # Log da exclusão
        logger.info(f"✅ Snapshot #{snapshot_id} excluído por admin (chave: ...{admin_key[-4:]})")
        logger.info(f"   Data do snapshot: {snapshot.created_at}")
        logger.info(f"   Registros de histórico removidos: {history_count}")
        
        # Limpa cache do ranking se excluiu um snapshot recente
        if is_latest:
            ranking_cache["data"] = None
            ranking_cache["timestamp"] = None
            logger.info("   Cache do ranking limpo (snapshot mais recente foi excluído)")
        
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "deleted_at": datetime.now(timezone.utc).isoformat(),
            "deleted_entries": history_count,
            "was_latest": is_latest,
            "remaining_snapshots": total_snapshots - 1,
            "message": f"Snapshot #{snapshot_id} e {history_count} registros de histórico excluídos com sucesso"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Erro ao excluir snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao excluir snapshot: {str(e)}")


@app.get("/ranking/snapshots/stats", tags=["ranking"])
async def get_snapshots_statistics(
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna estatísticas sobre os snapshots armazenados.
    Útil para decidir quais snapshots manter ou excluir.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        # Total de snapshots
        total_stmt = select(func.count(RankingSnapshot.id))
        total_snapshots = await db.scalar(total_stmt) or 0
        
        if total_snapshots == 0:
            return {
                "total_snapshots": 0,
                "message": "Nenhum snapshot encontrado"
            }
        
        # Snapshot mais antigo e mais recente
        oldest_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.asc()).limit(1)
        newest_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
        
        oldest_result = await db.execute(oldest_stmt)
        newest_result = await db.execute(newest_stmt)
        
        oldest = oldest_result.scalar_one_or_none()
        newest = newest_result.scalar_one_or_none()
        
        # Total de registros de histórico
        total_history_stmt = select(func.count(RankingHistory.id))
        total_history = await db.scalar(total_history_stmt) or 0
        
        # Estatísticas por período
        now = datetime.now(timezone.utc)
        
        # Snapshots por período
        last_24h_stmt = select(func.count(RankingSnapshot.id)).where(
            RankingSnapshot.created_at >= now - timedelta(hours=24)
        )
        last_7d_stmt = select(func.count(RankingSnapshot.id)).where(
            RankingSnapshot.created_at >= now - timedelta(days=7)
        )
        last_30d_stmt = select(func.count(RankingSnapshot.id)).where(
            RankingSnapshot.created_at >= now - timedelta(days=30)
        )
        
        snapshots_24h = await db.scalar(last_24h_stmt) or 0
        snapshots_7d = await db.scalar(last_7d_stmt) or 0
        snapshots_30d = await db.scalar(last_30d_stmt) or 0
        
        # Tamanho médio de snapshot (registros por snapshot)
        avg_size = total_history / total_snapshots if total_snapshots > 0 else 0
        
        # Distribuição de snapshots por mês
        monthly_stmt = text("""
            SELECT 
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as count
            FROM ranking_snapshots
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month DESC
            LIMIT 12
        """)
        
        monthly_result = await db.execute(monthly_stmt)
        monthly_distribution = []
        
        for row in monthly_result:
            monthly_distribution.append({
                "month": row.month.strftime("%Y-%m"),
                "count": row.count
            })
        
        return {
            "total_snapshots": total_snapshots,
            "total_history_records": total_history,
            "average_records_per_snapshot": round(avg_size, 1),
            "oldest_snapshot": {
                "id": oldest.id if oldest else None,
                "created_at": oldest.created_at.isoformat() if oldest else None,
                "age_days": (now - oldest.created_at).days if oldest else 0
            },
            "newest_snapshot": {
                "id": newest.id if newest else None,
                "created_at": newest.created_at.isoformat() if newest else None,
                "hours_ago": round((now - newest.created_at).total_seconds() / 3600, 1) if newest else 0
            },
            "snapshots_by_period": {
                "last_24_hours": snapshots_24h,
                "last_7_days": snapshots_7d,
                "last_30_days": snapshots_30d
            },
            "monthly_distribution": monthly_distribution,
            "storage_estimate": {
                "total_records": total_history,
                "estimated_size_mb": round((total_history * 500) / (1024 * 1024), 2),  # ~500 bytes por registro
                "note": "Estimativa baseada em ~500 bytes por registro"
            },
            "recommendations": _get_snapshot_recommendations(
                total_snapshots, snapshots_7d, snapshots_30d, oldest, newest
            )
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas de snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")


def _get_snapshot_recommendations(total: int, last_7d: int, last_30d: int, oldest, newest) -> List[str]:
    """Gera recomendações baseadas nas estatísticas de snapshots"""
    recommendations = []
    
    # Frequência de captura
    if last_7d > 14:
        recommendations.append("Considere reduzir a frequência de captura para economizar espaço")
    elif last_30d < 4:
        recommendations.append("Considere aumentar a frequência de captura para melhor histórico")
    
    # Snapshots antigos
    if oldest and (datetime.now(timezone.utc) - oldest.created_at).days > 365:
        recommendations.append("Existem snapshots com mais de 1 ano que podem ser excluídos")
    
    # Total de snapshots
    if total > 100:
        recommendations.append(f"Com {total} snapshots, considere uma limpeza mantendo os últimos 50-60")
    elif total < 10:
        recommendations.append("Mantenha capturando snapshots regularmente para construir histórico")
    
    # Intervalo ideal
    if newest:
        hours_since_last = (datetime.now(timezone.utc) - newest.created_at).total_seconds() / 3600
        if hours_since_last > 168:  # 7 dias
            recommendations.append("Há mais de uma semana sem novos snapshots")
    
    if not recommendations:
        recommendations.append("Sistema de snapshots está funcionando adequadamente")
    
    return recommendations

@app.get("/ranking/evolution", tags=["ranking"])
async def get_ranking_evolution(
    days: int = Query(30, ge=1, le=365, description="Número de dias para análise"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna uma análise da evolução do ranking nos últimos N dias,
    incluindo maiores subidas/descidas de posições e variações de nota.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        from ranking_history import get_ranking_evolution_summary
        
        evolution = await get_ranking_evolution_summary(db, days_back=days)
        return evolution
        
    except Exception as e:
        logger.error(f"Erro ao analisar evolução do ranking: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao analisar evolução: {str(e)}")


@app.get("/ranking/team/{team_id}/evolution", tags=["ranking"])
async def get_team_ranking_evolution(
    team_id: int,
    limit: int = Query(20, ge=1, le=100, description="Número de snapshots a analisar"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna a evolução detalhada de um time no ranking, incluindo
    variações de posição e nota entre snapshots consecutivos.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    try:
        from ranking_history import get_team_history
        
        # Busca histórico com variações
        history = await get_team_history(db, team_id, limit)
        
        # Calcula estatísticas da evolução
        if len(history) > 1:
            position_changes = [h["variacao"] for h in history if h.get("variacao") is not None]
            nota_changes = [h["variacao_nota"] for h in history if h.get("variacao_nota") is not None]
            
            evolution_stats = {
                "snapshots_analyzed": len(history),
                "position_changes": {
                    "biggest_rise": max(position_changes) if position_changes else 0,
                    "biggest_fall": min(position_changes) if position_changes else 0,
                    "average_change": round(sum(position_changes) / len(position_changes), 2) if position_changes else 0,
                    "total_rises": len([c for c in position_changes if c > 0]),
                    "total_falls": len([c for c in position_changes if c < 0]),
                    "no_change": len([c for c in position_changes if c == 0])
                },
                "nota_changes": {
                    "biggest_improvement": max(nota_changes) if nota_changes else 0,
                    "biggest_decline": min(nota_changes) if nota_changes else 0,
                    "average_change": round(sum(nota_changes) / len(nota_changes), 2) if nota_changes else 0,
                    "total_improvements": len([c for c in nota_changes if c > 0]),
                    "total_declines": len([c for c in nota_changes if c < 0])
                },
                "current_vs_oldest": {
                    "position_difference": history[0]["position"] - history[-1]["position"] if len(history) >= 2 else 0,
                    "nota_difference": round(history[0]["nota_final"] - history[-1]["nota_final"], 2) if len(history) >= 2 else 0
                }
            }
        else:
            evolution_stats = {
                "snapshots_analyzed": len(history),
                "message": "Dados insuficientes para análise de evolução"
            }
        
        return {
            "team": {
                "id": team.id,
                "name": team.name,
                "tag": team.tag,
                "university": team.university
            },
            "evolution_stats": evolution_stats,
            "history": history,
            "count": len(history)
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar evolução do time {team_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar evolução: {str(e)}")


@app.get("/ranking/movers", tags=["ranking"])
async def get_ranking_movers(
    days: int = Query(7, ge=1, le=90, description="Período em dias para análise"),
    limit: int = Query(10, ge=1, le=50, description="Número de times por categoria"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna os times que mais subiram/desceram posições e melhoraram/pioraram
    suas notas no período especificado.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        from ranking_history import get_ranking_evolution_summary
        
        evolution = await get_ranking_evolution_summary(db, days_back=days)
        
        if "error" in evolution:
            raise HTTPException(status_code=404, detail=evolution["error"])
        
        # Extrai apenas os top movers
        movers = evolution.get("top_movers", {})
        
        return {
            "period": {
                "days": days,
                "analysis_period": evolution.get("period_analysis", {})
            },
            "statistics": evolution.get("statistics", {}),
            "movers": {
                "biggest_risers": movers.get("biggest_risers", [])[:limit],
                "biggest_fallers": movers.get("biggest_fallers", [])[:limit],
                "biggest_nota_improvers": movers.get("biggest_nota_improvers", [])[:limit],
                "biggest_nota_decliners": movers.get("biggest_nota_decliners", [])[:limit]
            },
            "new_teams": evolution.get("new_teams", [])[:limit],
            "dropped_teams": evolution.get("dropped_teams", [])[:limit]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar movers do ranking: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar movers: {str(e)}")


@app.get("/ranking/snapshots/{snapshot_id}/details", tags=["ranking"])
async def get_snapshot_details(
    snapshot_id: int,
    include_comparison: bool = Query(False, description="Incluir comparação com snapshot anterior"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna detalhes de um snapshot específico, opcionalmente com
    comparação ao snapshot anterior.
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Busca o snapshot
    snapshot_stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
    snapshot_result = await db.execute(snapshot_stmt)
    snapshot = snapshot_result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot não encontrado")
    
    # Busca dados do ranking do snapshot
    history_stmt = text("""
        SELECT 
            rh.*,
            t.name,
            t.tag,
            t.university
        FROM ranking_history rh
        JOIN teams t ON rh.team_id = t.id
        WHERE rh.snapshot_id = :snapshot_id
        ORDER BY rh.position
    """)
    
    history_result = await db.execute(history_stmt, {"snapshot_id": snapshot_id})
    
    ranking_data = [_row_to_ranking_item(row) for row in history_result]
    
    response = {
        "snapshot": {
            "id": snapshot.id,
            "created_at": snapshot.created_at.isoformat(),
            "total_teams": snapshot.total_teams,
            "total_matches": snapshot.total_matches,
            "metadata": snapshot.snapshot_metadata
        },
        "ranking": ranking_data,
        "statistics": {
            "teams_count": len(ranking_data),
            "avg_nota": round(sum(r["nota_final"] for r in ranking_data) / len(ranking_data), 2) if ranking_data else 0,
            "max_nota": max(r["nota_final"] for r in ranking_data) if ranking_data else 0,
            "min_nota": min(r["nota_final"] for r in ranking_data) if ranking_data else 0,
            "anomalies_count": sum(r["anomaly"]["is_anomaly"] for r in ranking_data)
        }
    }
    
    # Adiciona comparação se solicitado
    if include_comparison:
        try:
            # Busca snapshot anterior
            prev_snapshot_stmt = (
                select(RankingSnapshot)
                .where(RankingSnapshot.created_at < snapshot.created_at)
                .order_by(RankingSnapshot.created_at.desc())
                .limit(1)
            )
            prev_result = await db.execute(prev_snapshot_stmt)
            prev_snapshot = prev_result.scalar_one_or_none()
            
            if prev_snapshot:
                from ranking_history import compare_snapshots
                comparison = await compare_snapshots(db, snapshot_id, prev_snapshot.id)
                response["comparison_with_previous"] = comparison
            else:
                response["comparison_with_previous"] = {
                    "message": "Nenhum snapshot anterior encontrado"
                }
                
        except Exception as e:
            logger.warning(f"Erro ao fazer comparação: {str(e)}")
            response["comparison_with_previous"] = {
                "error": str(e)
            }
    
    return response



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

async def calculate_ranking(
    db: AsyncSession,
    include_variation: bool = True,
    limit: Optional[int] = None,            # ← agora opcional aqui também
) -> List[Dict[str, Any]]:
    """
    Calcula o ranking completo e devolve lista de dicionários prontos para a API.

    • `include_variation`  – se True, compara com snapshot anterior
    • `limit`              – fatia o resultado final; qualquer valor inválido
                             (float, str, None) é ignorado com segurança
    """
    # ────────────────────── 0. Sanitiza `limit` ──────────────────────────
    try:
        limit = int(limit) if limit is not None else None
    except (TypeError, ValueError):
        limit = None

    # ────────────────────── 1. Coleta dados brutos ───────────────────────
    teams_q   = await db.execute(select(Team))
    teams     = teams_q.scalars().all()

    matches_q = await db.execute(
        select(Match)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date)
    )
    all_matches = list(matches_q.scalars())

    # Remove duplicatas por chave (teamA, teamB, data, mapa)
    match_keys: set[tuple] = set()
    unique_matches = []

    for m in all_matches:
        # descarta partidas sem times completos
        if not m.tmi_a or not m.tmi_b or not m.tmi_a.team or not m.tmi_b.team:
            continue

        # nomes dos times ordenados alfabeticamente
        names_tuple = tuple(sorted([
            m.tmi_a.team.name.strip(),
            m.tmi_b.team.name.strip()
        ]))

        # chave final é (teamA, teamB, data, mapa)  100 % tupla
        key = names_tuple + (
            m.date.strftime("%Y-%m-%d %H:%M"),
            m.map,
        )

        if key not in match_keys:
            match_keys.add(key)
            unique_matches.append(m)

    if not unique_matches:
        logger.warning("Nenhuma partida válida encontrada")
        return []


    # ────────────────────── 2. Calcula ­scores ───────────────────────────
    calculator  = RankingCalculator(teams, unique_matches)
    ranking_df  = calculator.calculate_final_ranking()
    ranking_df  = ranking_df.sort_values("NOTA_FINAL", ascending=False).reset_index(drop=True)

    # ────────────────────── 3. Snapshot anterior (variações) ─────────────
    previous_data: Dict[int, Dict[str, Any]] = {}
    if include_variation:
        try:
            last_snap_stmt = (
                select(RankingSnapshot)
                .order_by(RankingSnapshot.created_at.desc())
                .offset(1)                 # penúltimo snapshot
                .limit(1)
            )
            last_snap = (await db.execute(last_snap_stmt)).scalar_one_or_none()
            if last_snap:
                hist_q = await db.execute(
                    select(RankingHistory).where(RankingHistory.snapshot_id == last_snap.id)
                )
                for h in hist_q.scalars():
                    previous_data[h.team_id] = {
                        "position":  h.position,
                        "nota_final": float(h.nota_final),
                    }
        except Exception as e:
            logger.warning(f"Snapshot anterior indisponível: {e}")

    # ────────────────────── 4. Converte para lista de dicts ───────────────
    result: List[Dict[str, Any]] = []
    for idx, row in ranking_df.iterrows():
        posicao        = idx + 1
        team_id        = int(row.team_id)
        variacao       = None
        variacao_nota  = None
        is_new         = False

        if include_variation and team_id in previous_data:
            antes = previous_data[team_id]
            variacao      = antes["position"] - posicao
            variacao_nota = round(float(row.NOTA_FINAL) - antes["nota_final"], 2)
        elif include_variation:
            is_new = True

        result.append({
            "posicao":       posicao,
            "team_id":       team_id,
            "team":          row.team,
            "tag":           row.tag,
            "university":    row.university,
            "nota_final":    float(row.NOTA_FINAL),
            "ci_lower":      _f(row.ci_lower),
            "ci_upper":      _f(row.ci_upper),
            "incerteza":     _f(row.incerteza),
            "games_count":   int(row.games_count),
            "variacao":      variacao,
            "variacao_nota": variacao_nota,
            "is_new":        is_new,
            "scores": {
                "colley":        _f(row.r_colley),
                "massey":        _f(row.r_massey),
                "elo":           _f(row.r_elo_final),
                "elo_mov":       _f(row.r_elo_mov)       if "r_elo_mov" in row else None,
                "trueskill":     _f(row.ts_score),
                "pagerank":      _f(row.r_pagerank),
                "bradley_terry": _f(row.r_bradley_terry) if "r_bradley_terry" in row else None,
                "pca":           _f(row.pca_score),
                "sos":           _f(row.r_sos)           if "r_sos" in row else None,
                "consistency":   _f(row.r_consistency)   if "r_consistency" in row else None,
                "borda":         row.borda_score if "borda_score" in row else None,
                "integrado":     _f(row.rating_integrado),
            },
        })

    # ────────────────────── 5. Aplica `limit` com segurança ──────────────
    if limit is not None:
        result = result[: limit]

    logger.info(f"🏆 Ranking calculado com sucesso para {len(result)} times")
    return result

# ════════════════════════════════ ESTADOS ════════════════════════════════

@app.get("/estados", tags=["estados"])
async def list_estados(
    regiao: str = Query(None, description="Filtrar por região"),
    db: AsyncSession = Depends(get_db)
):
    """Lista todos os estados brasileiros"""
    from models import Estado
    
    stmt = select(Estado)
    if regiao:
        stmt = stmt.where(Estado.regiao == regiao)
    stmt = stmt.order_by(Estado.nome)
    
    result = await db.execute(stmt)
    estados = result.scalars().all()
    
    return {
        "total": len(estados),
        "estados": [
            {
                "id": e.id,
                "sigla": e.sigla,
                "nome": e.nome,
                "icone": e.icone,
                "regiao": e.regiao
            }
            for e in estados
        ]
    }

@app.get("/estados/{sigla}", tags=["estados"])
async def get_estado(
    sigla: str = Path(..., description="Sigla do estado (UF)"),
    db: AsyncSession = Depends(get_db)
):
    """Retorna informações de um estado específico"""
    from models import Estado
    
    stmt = select(Estado).where(Estado.sigla == sigla.upper())
    result = await db.execute(stmt)
    estado = result.scalar_one_or_none()
    
    if not estado:
        raise HTTPException(status_code=404, detail="Estado não encontrado")
    
    # Conta times do estado
    team_count_stmt = select(func.count(Team.id)).where(Team.estado_id == estado.id)
    team_count = await db.scalar(team_count_stmt)
    
    return {
        "id": estado.id,
        "sigla": estado.sigla,
        "nome": estado.nome,
        "icone": estado.icone,
        "regiao": estado.regiao,
        "teams_count": team_count
    }

@app.get("/estados/{sigla}/teams", tags=["estados", "teams"])
async def get_estado_teams(
    sigla: str = Path(..., description="Sigla do estado (UF)"),
    db: AsyncSession = Depends(get_db)
):
    """Lista todos os times de um estado"""
    from models import Estado
    
    # Busca o estado
    stmt = select(Estado).where(Estado.sigla == sigla.upper())
    result = await db.execute(stmt)
    estado = result.scalar_one_or_none()
    
    if not estado:
        raise HTTPException(status_code=404, detail="Estado não encontrado")
    
    # Busca times do estado
    teams_stmt = (
        select(Team)
        .where(Team.estado_id == estado.id)
        .order_by(Team.name)
    )
    teams_result = await db.execute(teams_stmt)
    teams = teams_result.scalars().all()
    
    return {
        "estado": {
            "id": estado.id,
            "sigla": estado.sigla,
            "nome": estado.nome,
            "icone": estado.icone,
            "regiao": estado.regiao
        },
        "teams_count": len(teams),
        "teams": [
            {
                "id": t.id,
                "name": t.name,
                "tag": t.tag,
                "university": t.university,
                "logo": t.logo
            }
            for t in teams
        ]
    }

@app.get("/regioes", tags=["estados"])
async def list_regioes(db: AsyncSession = Depends(get_db)):
    """Lista as regiões do Brasil com estatísticas"""
    from models import Estado
    
    stmt = text("""
        SELECT 
            e.regiao,
            COUNT(DISTINCT e.id) as estados_count,
            COUNT(DISTINCT t.id) as teams_count
        FROM estados e
        LEFT JOIN teams t ON t.estado_id = e.id
        GROUP BY e.regiao
        ORDER BY 
            CASE e.regiao
                WHEN 'Norte' THEN 1
                WHEN 'Nordeste' THEN 2
                WHEN 'Centro-Oeste' THEN 3
                WHEN 'Sudeste' THEN 4
                WHEN 'Sul' THEN 5
            END
    """)
    
    result = await db.execute(stmt)
    regioes = []
    
    for row in result:
        regioes.append({
            "regiao": row.regiao,
            "estados_count": row.estados_count,
            "teams_count": row.teams_count or 0
        })
    
    return {
        "total": len(regioes),
        "regioes": regioes
    }

@app.get("/estados/stats/summary", tags=["estados", "stats"])
async def get_estados_stats(db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas gerais sobre estados e distribuição de times"""
    from models import Estado
    
    # Estatísticas por estado
    stmt = text("""
        SELECT 
            e.sigla,
            e.nome,
            e.regiao,
            COUNT(t.id) as teams_count,
            COUNT(DISTINCT tm.id) as matches_count
        FROM estados e
        LEFT JOIN teams t ON t.estado_id = e.id
        LEFT JOIN team_match_info tmi ON tmi.team_id = t.id
        LEFT JOIN matches tm ON (tm.team_match_info_a = tmi.id OR tm.team_match_info_b = tmi.id)
        GROUP BY e.id, e.sigla, e.nome, e.regiao
        ORDER BY teams_count DESC, e.nome
    """)
    
    result = await db.execute(stmt)
    estados_data = []
    
    total_teams = 0
    estados_with_teams = 0
    
    for row in result:
        if row.teams_count > 0:
            estados_with_teams += 1
            total_teams += row.teams_count
            
        estados_data.append({
            "sigla": row.sigla,
            "nome": row.nome,
            "regiao": row.regiao,
            "teams_count": row.teams_count,
            "matches_count": row.matches_count or 0
        })
    
    # Top 5 estados
    top_estados = sorted(estados_data, key=lambda x: x["teams_count"], reverse=True)[:5]
    
    return {
        "summary": {
            "total_estados": len(estados_data),
            "estados_with_teams": estados_with_teams,
            "total_teams": total_teams,
            "avg_teams_per_estado": round(total_teams / estados_with_teams, 2) if estados_with_teams > 0 else 0
        },
        "top_5_estados": top_estados,
        "all_estados": estados_data
    }


@app.get("/teams/{team_id}/with-estado", response_model=schemas.Team, tags=["teams"])
async def get_team_with_estado(team_id: int, db: AsyncSession = Depends(get_db)):
    """Retorna detalhes de um time com informações completas do estado"""
    from models import Estado
    
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.id == team_id)
    )
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    return team