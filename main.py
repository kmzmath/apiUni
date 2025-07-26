# main.py
import os
from pathlib import Path as PathLib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, Depends, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select, text, func

from database import get_db, engine, Base
from models import Team, RankingSnapshot, RankingHistory, TeamPlayer, Match, Tournament, TeamMatchInfo
import crud
import schemas
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from sqlalchemy.orm import Session

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importações condicionais para o sistema de ranking
try:
    from ranking import calculate_ranking, RankingCalculator
    from ranking_history import save_ranking_snapshot, get_team_history, compare_snapshots
    import pandas as pd
    RANKING_AVAILABLE = True
    logger.info("✅ Sistema de ranking carregado com sucesso")
except ImportError as e:
    logger.warning(f"⚠️ Sistema de ranking não disponível: {e}")
    RANKING_AVAILABLE = False
    
    def save_ranking_snapshot(db): 
        return None
    
    def get_team_history(db, team_id, limit): 
        return []
    
    def calculate_ranking(db, include_variation=True):
        return []

def _f(v): 
    return float(v) if v is not None else None


# ───── SERIALIZAÇÃO PADRÃO DE RANKING ─────
def _row_to_ranking_item(row) -> dict:
    """Converte uma linha de SELECT (ranking_history JOIN teams)
    para o mesmo formato usado em /ranking."""
    return {
        "posicao":       row.position,
        "team_id":       row.team_id,
        "team":          row.name,
        "tag":           row.tag,
        "org":           row.org,
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
            "integrado":     _f(row.score_integrado),
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
def root():
    """Endpoint raiz da API"""
    return {
        "message": "API Valorant Universitário",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "online"
    }

@app.get("/health", response_class=PlainTextResponse, tags=["root"])
@app.head("/health", response_class=PlainTextResponse, include_in_schema=False)
def health_check():
    """
    Health check endpoint para monitoramento.
    Não toca no banco e devolve 200 a GET ou HEAD em <1 ms.
    """
    return PlainTextResponse("OK", status_code=200)

# ════════════════════════════════ TEAMS ════════════════════════════════

@app.get("/teams", response_model=List[schemas.Team], tags=["teams"])
def list_teams(db: Session = Depends(get_db)):
    """Lista todos os times ordenados alfabeticamente"""
    return crud.list_teams(db)

@app.get("/teams/search", response_model=List[schemas.Team], tags=["teams"])
def search_teams(
    q: str = Query(None, description="Buscar por nome, slug ou tag"),
    university: str = Query(None, description="Filtrar por universidade"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Busca times com filtros"""
    return crud.search_teams(db, query=q, university=university, limit=limit)

@app.get("/teams/by-slug/{slug}", tags=["teams"])
def get_team_by_slug(
    slug: str,
    complete: bool = Query(False, description="Retornar dados completos"),
    db: Session = Depends(get_db)
):
    """Busca um time pelo slug"""
    # Faz o join com a tabela estados para trazer as informações completas
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.slug == slug)
    )
    result = db.execute(stmt)
    team = result.scalar_one_or_none()
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    if complete:
        # Retorna dados completos usando o ID do time encontrado
        return get_team_complete_info(team.id, db)
    else:
        # Mantém comportamento atual - retorna dados formatados com estado
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
            "university": team.org,
            "university_tag": team.orgTag,
            "estado": team.estado,  # Campo string original (mantido para compatibilidade)
            "estado_info": estado_info,  # NOVO: Informações completas do estado
            "instagram": team.instagram,
            "twitch": team.twitch
        }

@app.get("/teams/{team_id}", tags=["teams"])
def get_team(
    team_id: int,
    complete: bool = Query(False, description="Retornar dados completos"),
    db: Session = Depends(get_db)
):
    if complete:
        # Retorna tudo que o endpoint /complete retornaria
        return get_team_complete_info(team_id, db)
    else:
        # Mantém comportamento atual
        team = crud.get_team(db, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Time não encontrado")
        return team

@app.get("/teams/{team_id}/matches", response_model=List[schemas.Match], tags=["teams"])
def get_team_matches(
    team_id: int,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Retorna todas as partidas de um time"""
    try:
        # Verifica se o time existe
        team = crud.get_team(db, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Time não encontrado")
        
        # Log para debug
        logger.info(f"Buscando partidas do time {team_id}: {team.name}")
        
        # Busca partidas com tratamento de erro
        try:
            matches = crud.get_team_matches(db, team_id, limit)
            logger.info(f"Encontradas {len(matches)} partidas para o time {team_id}")
            return matches
        except Exception as e:
            logger.error(f"Erro ao buscar partidas do time {team_id}: {str(e)}")
            logger.error(f"Tipo do erro: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Retorna lista vazia em caso de erro para não quebrar a API
            return []
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado em get_team_matches: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro interno ao buscar partidas")

@app.get("/teams/{team_id}/complete", tags=["teams"])
def get_team_complete_info(team_id: int, db: Session = Depends(get_db)):
    """Retorna informações completas de um time incluindo estatísticas"""
    team = crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    stats = crud.get_team_stats(db, team_id)
    
    # Busca jogadores do time
    players_stmt = text("""
        SELECT player_nick
        FROM team_players
        WHERE team_id = :team_id
        ORDER BY player_nick
    """)
    players_result = db.execute(players_stmt, {"team_id": team_id})
    players = [row[0] for row in players_result]
    
    # Informações do estado
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
        "team": {
            "id": team.id,
            "name": team.name,
            "logo": team.logo,
            "tag": team.tag,
            "slug": team.slug,
            "university": team.org,
            "university_tag": team.orgTag,
            "estado": team.estado,
            "estado_info": estado_info,
            "instagram": team.instagram,
            "twitch": team.twitch
        },
        "stats": stats,
        "players": players,
        "players_count": len(players)
    }

# ════════════════════════════════ ESTADOS ════════════════════════════════

@app.get("/estados", tags=["estados"])
def list_estados(db: Session = Depends(get_db)):
    """Lista todos os estados cadastrados"""
    from models import Estado
    
    stmt = select(Estado).order_by(Estado.regiao, Estado.nome)
    result = db.execute(stmt)
    estados = result.scalars().all()
    
    # Agrupa por região
    regioes = {}
    for estado in estados:
        if estado.regiao not in regioes:
            regioes[estado.regiao] = []
        
        # Conta times por estado
        team_count = db.scalar(
            select(func.count(Team.id)).where(Team.estado_id == estado.id)
        )
        
        regioes[estado.regiao].append({
            "id": estado.id,
            "sigla": estado.sigla,
            "nome": estado.nome,
            "icone": estado.icone,
            "teams_count": team_count
        })
    
    return regioes

@app.get("/estados/{estado_id}", tags=["estados"])
def get_estado(estado_id: int, db: Session = Depends(get_db)):
    """Retorna detalhes de um estado específico"""
    from models import Estado
    
    stmt = select(Estado).where(Estado.id == estado_id)
    result = db.execute(stmt)
    estado = result.scalar_one_or_none()
    
    if not estado:
        raise HTTPException(status_code=404, detail="Estado não encontrado")
    
    # Conta times do estado
    team_count_stmt = select(func.count(Team.id)).where(Team.estado_id == estado.id)
    team_count = db.scalar(team_count_stmt)
    
    return {
        "id": estado.id,
        "sigla": estado.sigla,
        "nome": estado.nome,
        "icone": estado.icone,
        "regiao": estado.regiao,
        "teams_count": team_count
    }

@app.get("/estados/{sigla}/teams", tags=["estados", "teams"])
def get_estado_teams(
    sigla: str = Path(..., description="Sigla do estado (UF)"),
    db: Session = Depends(get_db)
):
    """Lista todos os times de um estado"""
    from models import Estado
    
    # Busca o estado
    stmt = select(Estado).where(Estado.sigla == sigla.upper())
    result = db.execute(stmt)
    estado = result.scalar_one_or_none()
    
    if not estado:
        raise HTTPException(status_code=404, detail="Estado não encontrado")
    
    # Busca times do estado
    teams_stmt = (
        select(Team)
        .where(Team.estado_id == estado.id)
        .order_by(Team.name)
    )
    teams_result = db.execute(teams_stmt)
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
                "university": t.org,
                "logo": t.logo
            }
            for t in teams
        ]
    }

@app.get("/regioes", tags=["estados"])
def list_regioes(db: Session = Depends(get_db)):
    """Lista as regiões e contagem de times por região"""
    stmt = text("""
        SELECT 
            e.regiao,
            COUNT(DISTINCT e.id) as estados_count,
            COUNT(DISTINCT t.id) as teams_count
        FROM estados e
        LEFT JOIN teams t ON t.estado_id = e.id
        GROUP BY e.regiao
        ORDER BY e.regiao
    """)
    
    result = db.execute(stmt)
    
    return [
        {
            "regiao": row.regiao,
            "estados_count": row.estados_count,
            "teams_count": row.teams_count
        }
        for row in result
    ]

# ════════════════════════════════ PLAYERS ════════════════════════════════

@app.get("/players", tags=["players"])
def list_all_players(db: Session = Depends(get_db)):
    """Lista todos os jogadores cadastrados com seus times"""
    
    stmt = text("""
        SELECT 
            tp.player_nick,
            t.id  AS team_id,
            t.name AS team_name,
            t.tag  AS team_tag,
            t.logo AS team_logo
        FROM team_players tp
        JOIN teams t ON tp.team_id = t.id
        ORDER BY tp.player_nick, t.name
    """)
    
    result = db.execute(stmt)
    
    # Agrupa por jogador
    players_dict = {}
    for row in result:
        nick = row.player_nick
        if nick not in players_dict:
            players_dict[nick] = {
                "player_nick": nick,
                "teams": []
            }
        
        players_dict[nick]["teams"].append({
            "team_id": row.team_id,
            "team_name": row.team_name,
            "team_tag": row.team_tag,
            "team_logo": row.team_logo
        })
    
    return {
        "total_players": len(players_dict),
        "players": list(players_dict.values())
    }

@app.get("/teams/{team_id}/players", tags=["teams", "players"])
def get_team_players(team_id: int, db: Session = Depends(get_db)):
    """Lista jogadores de um time específico"""
    
    # Verifica se time existe
    team = crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    stmt = text("""
        SELECT player_nick
        FROM team_players
        WHERE team_id = :team_id
        ORDER BY player_nick
    """)
    
    result = db.execute(stmt, {"team_id": team_id})
    players = [row[0] for row in result]
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag
        },
        "players_count": len(players),
        "players": players
    }

@app.get("/players/teams", tags=["players"])
def get_teams_with_player_count(db: Session = Depends(get_db)):
    """Lista times com contagem de jogadores"""
    
    stmt = text("""
        SELECT 
            t.id,
            t.name,
            t.tag,
            t.logo,
            t.org,
            COUNT(tp.id) AS player_count
        FROM teams t
        LEFT JOIN team_players tp ON t.id = tp.team_id
        GROUP BY t.id, t.name, t.tag, t.logo, t.org
        ORDER BY player_count DESC, t.name
    """)
    
    result = db.execute(stmt)
    
    teams_data = []
    for row in result:
        teams_data.append({
            "id": row.id,
            "name": row.name,
            "tag": row.tag,
            "logo": row.logo,
            "org": row.org,
            "player_count": row.player_count
        })
    
    # Estatísticas gerais
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
def search_players(
    q: str = Query(..., min_length=2, description="Nome do jogador para buscar"),
    db: Session = Depends(get_db)
):
    """Busca jogadores por nome"""
    
    stmt = text("""
        SELECT DISTINCT
            tp.player_nick,
            t.id  AS team_id,
            t.name AS team_name,
            t.tag  AS team_tag,
            t.org  AS org
        FROM team_players tp
        JOIN teams t ON tp.team_id = t.id
        WHERE LOWER(tp.player_nick) LIKE LOWER(:search_term)
        ORDER BY tp.player_nick, t.name
        LIMIT 50
    """)
    
    result = db.execute(stmt, {"search_term": f"%{q}%"})
    
    players = []
    for row in result:
        players.append({
            "player_nick": row.player_nick,
            "team_id":     row.team_id,
            "team_name":   row.team_name,
            "team_tag":    row.team_tag,
            "org":         row.org
        })
    
    return {
        "query": q,
        "count": len(players),
        "players": players
    }

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

@app.get("/tournaments", response_model=List[schemas.Tournament], tags=["tournaments"])
def list_tournaments(db: Session = Depends(get_db)):
    """Lista todos os torneios ordenados por data de início"""
    return crud.list_tournaments(db)

@app.get("/tournaments/{tournament_id}", response_model=schemas.Tournament, tags=["tournaments"])
def get_tournament(tournament_id: uuid.UUID, db: Session = Depends(get_db)):
    """Retorna detalhes de um torneio específico"""
    tournament = crud.get_tournament(db, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Torneio não encontrado")
    return tournament

@app.get("/tournaments/{tournament_id}/matches", response_model=List[schemas.Match], tags=["tournaments"])
def get_tournament_matches(
    tournament_id: uuid.UUID,
    db: Session = Depends(get_db)
):
    """Retorna todas as partidas de um torneio"""
    tournament = crud.get_tournament(db, tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Torneio não encontrado")
    
    return crud.get_tournament_matches(db, tournament_id)

# ════════════════════════════════ MATCHES ════════════════════════════════

@app.get("/matches", response_model=List[schemas.Match], tags=["matches"])
def list_matches(
    limit: int = Query(20, ge=1, le=100, description="Número de partidas a retornar"),
    db: Session = Depends(get_db),
):
    """Retorna as partidas mais recentes com informações completas"""
    return crud.list_matches(db, limit=limit)

@app.get("/matches/{match_id}", response_model=schemas.Match, tags=["matches"])
def get_match(match_id: uuid.UUID, db: Session = Depends(get_db)):
    """Retorna detalhes de uma partida específica"""
    match = crud.get_match(db, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Partida não encontrada")
    return match

# ════════════════════════════════ STATS ════════════════════════════════

@app.get("/stats/maps", tags=["stats"])
def get_maps_stats(db: Session = Depends(get_db)):
    """Retorna estatísticas de mapas jogados"""
    return crud.get_maps_played(db)

@app.get("/stats/summary", tags=["stats"])
def get_general_stats(db: Session = Depends(get_db)):
    """Retorna estatísticas gerais do sistema"""
    try:
        # Contagens básicas
        teams_count = db.scalar(select(func.count(Team.id)))
        matches_count = db.scalar(select(func.count(Match.id)))
        tournaments_count = db.scalar(select(func.count(Tournament.id)))
        players_count = db.scalar(select(func.count(TeamPlayer.id)))
        
        # Times com mais vitórias
        stmt = text("""
            SELECT 
                t.id,
                t.name,
                t.tag,
                t.logo,
                COUNT(CASE 
                    WHEN (tmi_a.team_slug = t.slug AND tmi_a.score > tmi_b.score) OR
                         (tmi_b.team_slug = t.slug AND tmi_b.score > tmi_a.score)
                    THEN 1 
                END) as wins,
                COUNT(m.id) as total_matches
            FROM teams t
            LEFT JOIN team_match_info tmi_a ON tmi_a.team_slug = t.slug
            LEFT JOIN team_match_info tmi_b ON tmi_b.team_slug = t.slug
            LEFT JOIN matches m ON m.team_match_info_a = tmi_a.id OR m.team_match_info_b = tmi_b.id
            GROUP BY t.id, t.name, t.tag, t.logo
            HAVING COUNT(m.id) > 0
            ORDER BY wins DESC
            LIMIT 10
        """)
        
        result = db.execute(stmt)
        top_teams = []
        for row in result:
            winrate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
            top_teams.append({
                "id": row.id,
                "name": row.name,
                "tag": row.tag,
                "logo": row.logo,
                "wins": row.wins,
                "total_matches": row.total_matches,
                "winrate": round(winrate, 2)
            })
        
        # Partidas recentes
        recent_matches_stmt = (
            select(Match)
            .options(
                selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
                selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team)
            )
            .order_by(Match.date.desc())
            .limit(5)
        )
        recent_matches_result = db.execute(recent_matches_stmt)
        recent_matches = recent_matches_result.scalars().all()
        
        recent_matches_data = []
        for match in recent_matches:
            recent_matches_data.append({
                "id": str(match.id),
                "date": match.date.isoformat(),
                "team_a": {
                    "name": match.tmi_a.team.name if match.tmi_a.team else "Unknown",
                    "tag": match.tmi_a.team.tag if match.tmi_a.team else "",
                    "score": match.tmi_a.score
                },
                "team_b": {
                    "name": match.tmi_b.team.name if match.tmi_b.team else "Unknown",
                    "tag": match.tmi_b.team.tag if match.tmi_b.team else "",
                    "score": match.tmi_b.score
                },
                "map": match.mapa
            })
        
        return {
            "counts": {
                "teams": teams_count,
                "matches": matches_count,
                "tournaments": tournaments_count,
                "players": players_count
            },
            "top_teams": top_teams,
            "recent_matches": recent_matches_data
        }
    except Exception as e:
        logger.error(f"Erro em get_general_stats: {str(e)}")
        return {
            "counts": {
                "teams": 0,
                "matches": 0,
                "tournaments": 0,
                "players": 0
            },
            "top_teams": [],
            "recent_matches": []
        }

# ════════════════════════════════ RANKING ════════════════════════════════

@app.get("/ranking", tags=["ranking"])
def get_ranking(
    limit: int = Query(None, description="Limitar número de resultados"),
    db: Session = Depends(get_db)
):
    """
    Retorna o ranking calculado dos times.
    Se o sistema de ranking não estiver disponível, retorna lista vazia.
    """
    if not RANKING_AVAILABLE:
        return {
            "ranking": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "message": "Sistema de ranking não disponível"
        }
    
    # Verifica cache
    now = datetime.now(timezone.utc)
    if ranking_cache["data"] and ranking_cache["timestamp"]:
        if now - ranking_cache["timestamp"] < ranking_cache["ttl"]:
            logger.info("Retornando ranking do cache")
            data = ranking_cache["data"]
            if limit:
                data = data[:limit]
            return {
                "ranking": data,
                "generated_at": ranking_cache["timestamp"].isoformat(),
                "cached": True
            }
    
    # Calcula novo ranking
    try:
        logger.info("Calculando novo ranking...")
        ranking_data = calculate_ranking(db, include_variation=True)
        
        # Atualiza cache
        ranking_cache["data"] = ranking_data
        ranking_cache["timestamp"] = now
        
        if limit:
            ranking_data = ranking_data[:limit]
        
        return {
            "ranking": ranking_data,
            "generated_at": now.isoformat(),
            "cached": False
        }
    except Exception as e:
        logger.error(f"Erro ao calcular ranking: {str(e)}")
        return {
            "ranking": [],
            "generated_at": now.isoformat(),
            "available": False,
            "error": str(e)
        }

@app.get("/ranking/team/{team_id}", tags=["ranking"])
def get_team_ranking_history(
    team_id: int,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Retorna histórico de ranking de um time"""
    if not RANKING_AVAILABLE:
        return {
            "team_id": team_id,
            "history": [],
            "message": "Sistema de ranking não disponível"
        }
    
    # Verifica se time existe
    team = crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    try:
        history = get_team_history(db, team_id, limit)
        return {
            "team_id": team_id,
            "team_name": team.name,
            "history": history
        }
    except Exception as e:
        logger.error(f"Erro ao buscar histórico: {str(e)}")
        return {
            "team_id": team_id,
            "team_name": team.name,
            "history": []
        }

@app.get("/ranking/snapshots", tags=["ranking", "admin"])
def list_ranking_snapshots(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Lista os snapshots de ranking disponíveis"""
    stmt = (
        select(RankingSnapshot)
        .order_by(RankingSnapshot.created_at.desc())
        .limit(limit)
    )
    
    result = db.execute(stmt)
    snapshots = result.scalars().all()
    
    return [
        {
            "id": s.id,
            "created_at": s.created_at.isoformat(),
            "total_matches": s.total_matches,
            "total_teams": s.total_teams,
            "metadata": s.snapshot_metadata
        }
        for s in snapshots
    ]

@app.get("/ranking/snapshot/{snapshot_id}", tags=["ranking"])
def get_ranking_snapshot(
    snapshot_id: int,
    limit: int = Query(None, description="Limitar número de resultados"),
    db: Session = Depends(get_db)
):
    """Retorna um snapshot específico do ranking"""
    stmt = text("""
        SELECT 
            rh.position,
            rh.team_id,
            t.name,
            t.tag,
            t.org,
            rh.nota_final,
            rh.ci_lower,
            rh.ci_upper,
            rh.incerteza,
            rh.games_count,
            rh.score_colley,
            rh.score_massey,
            rh.score_elo_final,
            rh.score_elo_mov,
            rh.score_trueskill,
            rh.score_pagerank,
            rh.score_bradley_terry,
            rh.score_pca,
            rh.score_sos,
            rh.score_consistency,
            rh.score_integrado,
            rs.created_at AS snapshot_date
        FROM ranking_history rh
        JOIN teams t ON rh.team_id = t.id
        JOIN ranking_snapshots rs ON rh.snapshot_id = rs.id
        WHERE rh.snapshot_id = :snapshot_id
        ORDER BY rh.position
    """)
    
    if limit:
        stmt = text(str(stmt) + f" LIMIT {limit}")
    
    result = db.execute(stmt, {"snapshot_id": snapshot_id})
    rows = result.fetchall()
    
    if not rows:
        raise HTTPException(status_code=404, detail="Snapshot não encontrado")
    
    ranking_data = [_row_to_ranking_item(row) for row in rows]
    
    return {
        "snapshot_id": snapshot_id,
        "created_at": rows[0].snapshot_date.isoformat() if rows else None,
        "ranking": ranking_data
    }

# ════════════════════════════════ ADMIN ════════════════════════════════

@app.post("/admin/ranking/snapshot", tags=["admin", "ranking"])
def create_ranking_snapshot(
    db: Session = Depends(get_db),
    force: bool = Query(False, description="Forçar criação mesmo se houver snapshot recente")
):
    """Cria um novo snapshot do ranking atual"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica se já existe snapshot recente
    if not force:
        stmt = (
            select(RankingSnapshot)
            .order_by(RankingSnapshot.created_at.desc())
            .limit(1)
        )
        result = db.execute(stmt)
        last_snapshot = result.scalar_one_or_none()
        
        if last_snapshot:
            time_diff = datetime.now(timezone.utc) - last_snapshot.created_at
            if time_diff < timedelta(hours=6):
                raise HTTPException(
                    status_code=400, 
                    detail=f"Snapshot criado há {time_diff.total_seconds() / 3600:.1f} horas. Use force=true para forçar."
                )
    
    try:
        snapshot_id = save_ranking_snapshot(db)
        db.commit()
        
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Erro ao criar snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar snapshot: {str(e)}")

@app.delete("/admin/cache/ranking", tags=["admin", "ranking"])
def clear_ranking_cache():
    """Limpa o cache do ranking"""
    ranking_cache["data"] = None
    ranking_cache["timestamp"] = None
    
    return {
        "success": True,
        "message": "Cache do ranking limpo com sucesso"
    }

# ════════════════════════════════ DEBUG ════════════════════════════════

@app.get("/debug/team/{team_id}/check", tags=["debug"])
def debug_team_check(team_id: int, db: Session = Depends(get_db)):
    """Debug endpoint para verificar problemas com dados de um time"""
    try:
        response = {
            "team_id": team_id,
            "checks": {},
            "errors": []
        }
        
        # 1. Verifica se time existe
        team = crud.get_team(db, team_id)
        if not team:
            response["errors"].append("Time não encontrado")
            return response
        
        response["team"] = {
            "id": team.id,
            "name": team.name,
            "slug": team.slug,
            "tag": team.tag
        }
        
        # 2. Verifica se tem slug
        if not team.slug:
            response["errors"].append("Time sem slug definido")
            return response
        
        # 3. Conta team_match_info
        tmi_count_stmt = text("""
            SELECT COUNT(*) 
            FROM team_match_info 
            WHERE team_slug = :slug
        """)
        tmi_count = db.scalar(tmi_count_stmt, {"slug": team.slug})
        response["team_match_info_count"] = tmi_count
        
        # 4. Conta partidas onde o time aparece
        match_count_stmt = text("""
            SELECT COUNT(DISTINCT m.id)
            FROM matches m
            JOIN team_match_info tmi ON (
                m.team_match_info_a = tmi.id OR 
                m.team_match_info_b = tmi.id
            )
            WHERE tmi.team_slug = :slug
        """)
        match_count = db.scalar(match_count_stmt, {"slug": team.slug})
        response["match_count"] = match_count
        
        # 5. Tenta buscar uma partida completa
        try:
            matches = crud.get_team_matches(db, team_id, limit=1)
            response["get_team_matches_success"] = True
            response["matches_returned"] = len(matches)
        except Exception as e:
            response["get_team_matches_success"] = False
            response["get_team_matches_error"] = str(e)
            response["errors"].append(f"Erro em get_team_matches: {str(e)}")
            
            # Tenta query mais simples
            try:
                simple_query = text("""
                    SELECT m.id, m.mapa, m.date
                    FROM matches m
                    JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
                    JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
                    WHERE tmi_a.team_slug = :slug OR tmi_b.team_slug = :slug
                    LIMIT 1
                """)
                simple_result = db.execute(simple_query, {"slug": team.slug})
                simple_match = simple_result.first()
                if simple_match:
                    response["simple_query_success"] = True
                    response["sample_match"] = {
                        "id": str(simple_match.id),
                        "map": simple_match.mapa,
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

@app.get("/info", tags=["root"])
def get_api_info(db: Session = Depends(get_db)):
    """
    Retorna informações sobre a API e o estado do sistema.
    """
    
    # Informações do último snapshot
    last_snapshot = None
    if RANKING_AVAILABLE:
        try:
            stmt = (
                select(RankingSnapshot)
                .order_by(RankingSnapshot.created_at.desc())
                .limit(1)
            )
            result = db.execute(stmt)
            snapshot = result.scalar_one_or_none()
            if snapshot:
                last_snapshot = {
                    "id": snapshot.id,
                    "created_at": snapshot.created_at.isoformat(),
                    "total_teams": snapshot.total_teams,
                    "total_matches": snapshot.total_matches
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

def calculate_ranking(
    db: Session,
    include_variation: bool = True,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Calcula o ranking completo e devolve lista de dicionários prontos para a API.
    """
    # ────────────────────── 0. Sanitiza `limit` ──────────────────────────
    try:
        limit = int(limit) if limit is not None else None
    except (TypeError, ValueError):
        limit = None

    # ────────────────────── 1. Coleta dados brutos ───────────────────────
    teams_q = db.execute(select(Team))
    teams = teams_q.scalars().all()

    matches_q = db.execute(
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
    match_keys = set()
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

        # chave final é (teamA, teamB, data, mapa)  100 % tupla
        key = names_tuple + (
            m.date.strftime("%Y-%m-%d %H:%M"),
            m.mapa,
        )

        if key not in match_keys:
            match_keys.add(key)
            unique_matches.append(m)

    if not unique_matches:
        logger.warning("Nenhuma partida válida encontrada")
        return []

    # ────────────────────── 2. Calcula ranking ───────────────────────
    try:
        calculator = RankingCalculator(teams, unique_matches)
        ranking_df = calculator.calculate_all()
        
        # Converte para lista de dicionários
        ranking_data = []
        for _, row in ranking_df.iterrows():
            team_id = next((t.id for t in teams if t.name == row['team']), None)
            
            ranking_data.append({
                "posicao": int(row['posicao']),
                "team_id": team_id,
                "team": row['team'],
                "tag": row.get('tag', ''),
                "org": row.get('org', ''),
                "nota_final": float(row['nota_final']),
                "ci_lower": float(row['ci_lower']),
                "ci_upper": float(row['ci_upper']),
                "incerteza": float(row['incerteza']),
                "games_count": int(row['games_count']),
                "variacao": None,
                "variacao_nota": None,
                "is_new": False,
                "scores": {
                    "colley": float(row.get('score_colley', 0)),
                    "massey": float(row.get('score_massey', 0)),
                    "elo": float(row.get('score_elo_final', 0)),
                    "elo_mov": float(row.get('score_elo_mov', 0)),
                    "trueskill": float(row.get('score_trueskill', 0)),
                    "pagerank": float(row.get('score_pagerank', 0)),
                    "bradley_terry": float(row.get('score_bradley_terry', 0)),
                    "pca": float(row.get('score_pca', 0)),
                    "sos": float(row.get('score_sos', 0)),
                    "consistency": float(row.get('score_consistency', 0)),
                    "integrado": float(row.get('score_integrado', 0)),
                }
            })
        
        # Se include_variation, adiciona variações
        if include_variation:
            compare_snapshots(db, ranking_data)
        
        # Aplica limite se especificado
        if limit and limit > 0:
            ranking_data = ranking_data[:limit]
        
        return ranking_data
        
    except Exception as e:
        logger.error(f"Erro ao calcular ranking: {str(e)}")
        return []

def compare_snapshots(db: Session, current_ranking: List[Dict[str, Any]]):
    """Adiciona informações de variação comparando com snapshot anterior"""
    try:
        # Busca penúltimo snapshot
        stmt = (
            select(RankingSnapshot)
            .order_by(RankingSnapshot.created_at.desc())
            .offset(1)
            .limit(1)
        )
        result = db.execute(stmt)
        previous_snapshot = result.scalar_one_or_none()
        
        if not previous_snapshot:
            return
        
        # Busca dados do snapshot anterior
        prev_stmt = text("""
            SELECT 
                team_id,
                position,
                nota_final
            FROM ranking_history
            WHERE snapshot_id = :snapshot_id
        """)
        
        prev_result = db.execute(prev_stmt, {"snapshot_id": previous_snapshot.id})
        prev_data = {row.team_id: row for row in prev_result}
        
        # Adiciona variações
        for item in current_ranking:
            team_id = item["team_id"]
            if team_id in prev_data:
                prev = prev_data[team_id]
                item["variacao"] = prev.position - item["posicao"]
                item["variacao_nota"] = float(item["nota_final"] - prev.nota_final)
            else:
                item["is_new"] = True
                
    except Exception as e:
        logger.error(f"Erro ao comparar snapshots: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)