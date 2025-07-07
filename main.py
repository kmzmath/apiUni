# main.py
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, delete, func
from sqlalchemy.orm import selectinload

from database import get_db, engine, Base
from models import Team, RankingSnapshot, RankingHistory, TeamPlayer, Match, Tournament
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
    from ranking import calculate_ranking
    from ranking_history import save_ranking_snapshot, get_team_history
    RANKING_AVAILABLE = True
    logger.info("✅ Sistema de ranking carregado com sucesso")
except ImportError as e:
    logger.warning(f"⚠️ Sistema de ranking não disponível: {e}")
    RANKING_AVAILABLE = False
    
    async def save_ranking_snapshot(db): 
        return None
    
    async def get_team_history(db, team_id, limit): 
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
        {"name": "players", "description": "Operações com jogadores"},
        {"name": "tournaments", "description": "Operações com torneios"},
        {"name": "matches", "description": "Operações com partidas"},
        {"name": "ranking", "description": "Sistema de ranking"},
        {"name": "stats", "description": "Estatísticas gerais"},
        {"name": "debug", "description": "Endpoints de debug (apenas desenvolvimento)"}
    ]
)

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ════════════════════════════════ STARTUP & SHUTDOWN ════════════════════════════════

@app.on_event("startup")
async def startup():
    """Verifica conexão com banco de dados no startup"""
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("✅ Conexão com banco de dados estabelecida!")
        
        # Verifica contagem de registros principais
        async with AsyncSession(engine) as db:
            teams_count = await db.scalar(select(func.count(Team.id)))
            matches_count = await db.scalar(select(func.count(Match.id)))
            logger.info(f"📊 Banco de dados: {teams_count} times, {matches_count} partidas")
            
    except Exception as e:
        logger.error(f"❌ Erro ao conectar com banco: {e}")
        raise

@app.on_event("shutdown")
async def shutdown():
    """Cleanup ao desligar a aplicação"""
    logger.info("👋 Encerrando aplicação...")
    await engine.dispose()

# ════════════════════════════════ ROOT & HEALTH ════════════════════════════════

@app.get("/", tags=["root"])
async def root():
    """Endpoint raiz com informações da API"""
    return {
        "name": "Valorant Universitário API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "ranking_available": RANKING_AVAILABLE,
        "endpoints": {
            "teams": "/teams",
            "tournaments": "/tournaments", 
            "matches": "/matches",
            "ranking": "/ranking" if RANKING_AVAILABLE else "disabled",
            "stats": "/stats/maps",
            "players": "/players/search"
        }
    }

@app.get("/health", tags=["root"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """Verifica a saúde da API e conexão com banco"""
    try:
        # Testa conexão
        await db.execute(text("SELECT 1"))
        
        # Busca contagens
        team_count = await db.scalar(select(func.count(Team.id)))
        match_count = await db.scalar(select(func.count(Match.id)))
        
        # Verifica último snapshot se ranking disponível
        latest_snapshot = None
        if RANKING_AVAILABLE:
            stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
            result = await db.execute(stmt)
            snapshot = result.scalar_one_or_none()
            if snapshot:
                latest_snapshot = {
                    "id": snapshot.id,
                    "created_at": snapshot.created_at.isoformat(),
                    "age_hours": (datetime.utcnow() - snapshot.created_at.replace(tzinfo=None)).total_seconds() / 3600
                }
        
        return {
            "status": "healthy",
            "database": {
                "connected": True,
                "teams": team_count,
                "matches": match_count
            },
            "ranking": {
                "available": RANKING_AVAILABLE,
                "latest_snapshot": latest_snapshot
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": {
                "connected": False,
                "error": str(e)
            },
            "ranking": {
                "available": RANKING_AVAILABLE
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/ping", tags=["root"])
async def ping():
    """Keep-alive endpoint para evitar hibernação"""
    return {"ok": True, "timestamp": datetime.now(timezone.utc).isoformat()}

# ───────────────────── UPTIME MONITOR ─────────────────────
from fastapi.responses import PlainTextResponse         # já deve estar importado; senão, adicione

@app.api_route("/uptime", methods=["GET", "HEAD"], tags=["root"])
async def uptime() -> PlainTextResponse:
    """
    Endpoint exclusivo para monitoramento externo (UptimeRobot).
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
        "university_tag": team.university_tag
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
        "university": team.university
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
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "university": team.university
        },
        "player_count": len(players),
        "players": players
    }

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
    """Retorna todos os torneios que o time participou"""
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Query para buscar torneios únicos
    stmt = text("""
        SELECT DISTINCT
            t.id,
            t.name,
            t.logo,
            t.organizer,
            t.starts_on,
            t.ends_on,
            COUNT(DISTINCT m.id) as matches_played,
            MIN(m.date) as first_match,
            MAX(m.date) as last_match,
            -- Calcula se ganhou ou perdeu mais partidas
            SUM(CASE 
                WHEN (m.team_match_info_a = tmi.id AND tmi.score > tmi_opponent.score) OR
                     (m.team_match_info_b = tmi.id AND tmi.score > tmi_opponent.score)
                THEN 1 ELSE 0 
            END) as wins,
            COUNT(DISTINCT m.id) as total_matches
        FROM tournaments t
        JOIN matches m ON m.tournament_id = t.id
        JOIN team_match_info tmi ON (
            m.team_match_info_a = tmi.id OR 
            m.team_match_info_b = tmi.id
        )
        JOIN team_match_info tmi_opponent ON (
            CASE 
                WHEN m.team_match_info_a = tmi.id THEN m.team_match_info_b = tmi_opponent.id
                ELSE m.team_match_info_a = tmi_opponent.id
            END
        )
        WHERE tmi.team_id = :team_id
        GROUP BY t.id, t.name, t.logo, t.organizer, t.starts_on, t.ends_on
        ORDER BY MAX(m.date) DESC
    """)
    
    result = await db.execute(stmt, {"team_id": team_id})
    
    tournaments = []
    for row in result:
        win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
        tournaments.append({
            "id": str(row.id),
            "name": row.name,
            "logo": row.logo,
            "organizer": row.organizer,
            "starts_on": row.starts_on.isoformat() if row.starts_on else None,
            "ends_on": row.ends_on.isoformat() if row.ends_on else None,
            "matches_played": row.matches_played,
            "first_match": row.first_match.isoformat() if row.first_match else None,
            "last_match": row.last_match.isoformat() if row.last_match else None,
            "wins": row.wins,
            "losses": row.total_matches - row.wins,
            "win_rate": round(win_rate, 1),
            "status": "active" if row.ends_on and row.ends_on > datetime.now() else "finished"
        })
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag
        },
        "total_tournaments": len(tournaments),
        "tournaments": tournaments
    }

@app.get("/teams/{team_id}/full", tags=["teams"])
async def get_team_full_info(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna todas as informações necessárias para a página da equipe"""
    
    # Busca dados básicos
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca jogadores
    players_result = await db.execute(
        select(TeamPlayer.player_nick)
        .where(TeamPlayer.team_id == team_id)
        .order_by(TeamPlayer.id)
    )
    players = [row[0] for row in players_result]
    
    # Busca posição atual no ranking
    current_ranking = None
    if RANKING_AVAILABLE:
        ranking_data = await calculate_ranking(db, include_variation=True)
        for item in ranking_data:
            if item.get("team_id") == team_id:
                current_ranking = {
                    "position": item["posicao"],
                    "nota_final": item["nota_final"],
                    "variation": item.get("variacao"),
                    "games_count": item["games_count"]
                }
                break
    
    # Busca estatísticas
    stats = await crud.get_team_stats(db, team_id)
    
    # Busca últimas 5 partidas
    recent_matches = await crud.get_team_matches(db, team_id, limit=5)
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "slug": team.slug,
            "logo": team.logo,
            "university": team.university,
            "university_tag": team.university_tag,
            "social_media": {
                "instagram": team.instagram,
                "twitter": team.twitter,
                "discord": team.discord,
                "twitch": team.twitch,
                "youtube": team.youtube
            }
        },
        "players": players,
        "current_ranking": current_ranking,
        "stats": stats,
        "recent_matches": recent_matches
    }

# ════════════════════════════════ PLAYERS ════════════════════════════════

@app.get("/teams/players/summary", tags=["players"])
async def get_teams_players_summary(db: AsyncSession = Depends(get_db)):
    """Retorna resumo de todos os times com seus jogadores"""
    
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
            HAVING COUNT(*) >= 5
            ORDER BY wins DESC
            LIMIT 5
        """)
        
        result = await db.execute(stmt)
        top_winners = []
        for row in result:
            win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
            top_winners.append({
                "team_id": row.id,
                "team_name": f"{row.name} ({row.tag})",
                "wins": row.wins,
                "total_matches": row.total_matches,
                "win_rate": round(win_rate, 1)
            })
        
        return {
            "totals": {
                "teams": teams_count,
                "matches": matches_count,
                "tournaments": tournaments_count,
                "players": players_count
            },
            "top_winners": top_winners,
            "ranking_available": RANKING_AVAILABLE,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas gerais: {e}")
        raise HTTPException(status_code=500, detail="Erro ao calcular estatísticas")

# ════════════════════════════════ RANKING ════════════════════════════════

@app.get("/ranking", tags=["ranking"])
async def get_ranking(
    db: AsyncSession = Depends(get_db),
    force_refresh: bool = Query(False, description="Forçar recálculo do ranking"),
    limit: Optional[int] = Query(None, ge=1, le=100, description="Limitar número de resultados")
):
    """
    Retorna o ranking completo dos times
    
    Algoritmos utilizados:
    - Colley, Massey, Elo, TrueSkill, PageRank, Bradley-Terry, PCA
    
    Cache: 1 hora (use force_refresh=true para recalcular)
    """
    if not RANKING_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Sistema de ranking não disponível. Instale as dependências científicas."
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
                "total_matches": s.total_matches,
                "total_teams": s.total_teams,
                "metadata": s.snapshot_metadata
            }
            for s in snapshots
        ],
        "count": len(snapshots),
        "limit": limit
    }

@app.get("/ranking/snapshots/latest", tags=["ranking"])
async def get_latest_snapshot(db: AsyncSession = Depends(get_db)):
    """Retorna informações sobre o último snapshot capturado"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
    result = await db.execute(stmt)
    latest = result.scalar_one_or_none()
    
    if not latest:
        return {"message": "Nenhum snapshot encontrado", "latest": None}
    
    time_since = datetime.utcnow() - latest.created_at.replace(tzinfo=None)
    hours_ago = time_since.total_seconds() / 3600
    days_ago = hours_ago / 24
    
    # Busca estatísticas do snapshot
    stmt = select(
        func.count(RankingHistory.id),
        func.avg(RankingHistory.nota_final),
        func.max(RankingHistory.nota_final),
        func.min(RankingHistory.nota_final)
    ).where(RankingHistory.snapshot_id == latest.id)
    
    result = await db.execute(stmt)
    count, avg_nota, max_nota, min_nota = result.first()
    
    return {
        "latest": {
            "id": latest.id,
            "created_at": latest.created_at.isoformat(),
            "total_matches": latest.total_matches,
            "total_teams": latest.total_teams,
            "metadata": latest.snapshot_metadata,
            "stats": {
                "teams_ranked": count or 0,
                "avg_nota": float(avg_nota) if avg_nota else 0,
                "max_nota": float(max_nota) if max_nota else 0,
                "min_nota": float(min_nota) if min_nota else 0
            }
        },
        "time_since": {
            "hours": round(hours_ago, 1),
            "days": round(days_ago, 1),
            "human_readable": f"{round(days_ago)} dias atrás" if days_ago >= 1 else f"{round(hours_ago)} horas atrás"
        }
    }

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
        previous_notas = {}
        
        if include_variation:
            try:
                from models import RankingSnapshot, RankingHistory
                
                # Busca o último snapshot
                snapshot_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).offset(1).limit(1)
                snapshot_result = await db.execute(snapshot_stmt)
                last_snapshot = snapshot_result.scalar_one_or_none()
                
                if last_snapshot:
                    # Busca as posições e notas do último snapshot
                    history_stmt = (
                        select(RankingHistory)
                        .where(RankingHistory.snapshot_id == last_snapshot.id)
                    )
                    history_result = await db.execute(history_stmt)
                    
                    for history_entry in history_result.scalars():
                        previous_positions[history_entry.team_id] = history_entry.position
                        previous_notas[history_entry.team_id] = float(history_entry.nota_final)
                    
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
            variacao_nota = None
            is_new = False
            
            if include_variation and pd.notna(row.team_id):
                team_id_int = int(row.team_id)
                
                # Variação de posição
                if team_id_int in previous_positions:
                    posicao_anterior = previous_positions[team_id_int]
                    variacao = posicao_anterior - position  # Positivo = subiu, Negativo = desceu
                    
                    # Variação de nota
                    if team_id_int in previous_notas:
                        nota_anterior = previous_notas[team_id_int]
                        variacao_nota = float(row.NOTA_FINAL) - nota_anterior
                else:
                    # Time não estava no ranking anterior - é novo!
                    is_new = True
                    logger.debug(f"Time {row.team} é NOVO no ranking")
            
            # Monta o dicionário do time
            team_data = {
                "posicao": position,
                "team": row.team.strip() if pd.notna(row.team) else "Unknown",
                "tag": row.tag.strip() if pd.notna(row.tag) else "",
                "university": row.university.strip() if pd.notna(row.university) else "Unknown",
                "team_id": int(row.team_id) if pd.notna(row.team_id) else None,
                "nota_final": float(row.NOTA_FINAL),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": int(row.games_count),
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
                    "borda": float(row.borda_score),
                    "integrado": float(row.integrado_score)
                },
                "anomaly": {
                    "is_anomaly": bool(row.is_anomaly),
                    "score": float(row.anomaly_score)
                },
                "variacao": variacao,
                "variacao_nota": variacao_nota,  # CAMPO ADICIONADO
                "is_new": is_new
            }
            
            result.append(team_data)
        
        logger.info(f"✅ Ranking calculado com sucesso: {len(result)} times")
        return result
        
    except Exception as e:
        logger.error(f"❌ Erro ao calcular ranking: {str(e)}", exc_info=True)
        raise

@app.post("/ranking/snapshot", tags=["ranking"])
async def create_ranking_snapshot(
    db: AsyncSession = Depends(get_db),
    secret_key: str = Query(..., description="Chave para autorizar snapshot")
):
    """Cria um snapshot do ranking atual"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    if secret_key != os.getenv("RANKING_REFRESH_KEY", "valorant2024ranking"):
        raise HTTPException(status_code=403, detail="Chave inválida")
    
    try:
        # Cria o snapshot
        snapshot_id = await save_ranking_snapshot(db)
        
        if not snapshot_id:
            raise HTTPException(status_code=500, detail="Falha ao criar snapshot")
        
        # Limpa cache do ranking
        ranking_cache["data"] = None
        ranking_cache["timestamp"] = None
        
        # Busca informações do snapshot criado
        stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
        result = await db.execute(stmt)
        snapshot = result.scalar_one_or_none()
        
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "message": "Snapshot do ranking criado com sucesso",
            "status": "completed",
            "teams": snapshot.total_teams if snapshot else 0,
            "matches": snapshot.total_matches if snapshot else 0,
            "ranking_available": True
        }
    except Exception as e:
        logger.error(f"Erro ao criar snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar snapshot: {str(e)}")

@app.get("/ranking/history/{snapshot_id}", tags=["ranking"])
async def get_ranking_history_by_snapshot(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna o ranking de um snapshot específico"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Busca o snapshot
    stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
    result = await db.execute(stmt)
    snapshot = result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} não encontrado")
    
    # Busca o snapshot anterior para calcular variações
    stmt_prev = (
        select(RankingSnapshot)
        .where(RankingSnapshot.created_at < snapshot.created_at)
        .order_by(RankingSnapshot.created_at.desc())
        .limit(1)
    )
    result_prev = await db.execute(stmt_prev)
    previous_snapshot = result_prev.scalar_one_or_none()
    
    # Se temos um snapshot anterior, busca as posições e notas anteriores
    previous_positions = {}
    previous_notas = {}
    
    if previous_snapshot:
        stmt_prev_data = (
            select(RankingHistory.team_id, RankingHistory.position, RankingHistory.nota_final)
            .where(RankingHistory.snapshot_id == previous_snapshot.id)
        )
        result_prev_data = await db.execute(stmt_prev_data)
        
        for team_id, position, nota_final in result_prev_data:
            previous_positions[team_id] = position
            previous_notas[team_id] = nota_final
            
        logger.info(f"📊 Comparando snapshot #{snapshot_id} com snapshot #{previous_snapshot.id}")
    else:
        logger.info(f"📊 Snapshot #{snapshot_id} é o primeiro snapshot, sem comparação disponível")
    
    # Busca os dados históricos do ranking
    stmt = (
        select(
            RankingHistory,
            Team.name,
            Team.tag,
            Team.university,
            Team.logo
        )
        .join(Team, RankingHistory.team_id == Team.id)
        .where(RankingHistory.snapshot_id == snapshot_id)
        .order_by(RankingHistory.position)
    )
    
    result = await db.execute(stmt)
    history_data = result.all()
    
    rankings = []
    for history, team_name, team_tag, university, logo in history_data:
        # Calcular variação de posição em relação ao snapshot anterior
        variacao = None
        variacao_nota = None
        is_new = False
        
        if previous_positions:  # Se temos um snapshot anterior
            if history.team_id in previous_positions:
                posicao_anterior = previous_positions[history.team_id]
                variacao = posicao_anterior - history.position  # Positivo = subiu
                
                # Calcular variação de nota
                if history.team_id in previous_notas:
                    nota_anterior = previous_notas[history.team_id]
                    variacao_nota = float(history.nota_final) - float(nota_anterior)
                    
                logger.debug(f"Time {team_name}: posição anterior={posicao_anterior}, atual={history.position}, variação={variacao}")
                logger.debug(f"Time {team_name}: nota anterior={nota_anterior:.3f}, atual={history.nota_final:.3f}, variação_nota={variacao_nota:.3f}")
            else:
                # Time não estava no snapshot anterior
                is_new = True
                logger.debug(f"Time {team_name} é NOVO no ranking")
        
        rankings.append({
            "position": history.position,
            "team_id": history.team_id,
            "team_name": f"{team_name} ({team_tag})",
            "university": university,
            "logo": logo,
            "nota_final": history.nota_final,
            "ci_lower": history.ci_lower,
            "ci_upper": history.ci_upper,
            "incerteza": history.incerteza,
            "games_count": history.games_count,
            "scores": {
                "colley": history.score_colley,
                "massey": history.score_massey,
                "elo": history.score_elo_final,
                "elo_mov": history.score_elo_mov,
                "trueskill": history.score_trueskill,
                "pagerank": history.score_pagerank,
                "bradley_terry": history.score_bradley_terry,
                "pca": history.score_pca,
                "sos": history.score_sos,
                "consistency": history.score_consistency,
                "borda": history.score_borda,
                "integrado": history.score_integrado
            },
            "variacao": variacao,
            "variacao_nota": variacao_nota,  # CAMPO ADICIONADO
            "is_new": is_new
        })
    
    return {
        "snapshot_id": snapshot.id,
        "created_at": snapshot.created_at.isoformat(),
        "total_teams": snapshot.total_teams,
        "total_matches": snapshot.total_matches,
        "metadata": snapshot.snapshot_metadata,
        "rankings": rankings,
        "compared_with_snapshot": previous_snapshot.id if previous_snapshot else None
    }


@app.delete("/ranking/snapshot/{snapshot_id}", tags=["ranking"])
async def delete_snapshot(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
    secret_key: str = Query(..., description="Chave para autorizar exclusão")
):
    """Exclui um snapshot e todos os seus dados históricos"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    # Verifica a chave secreta
    if secret_key != os.getenv("RANKING_REFRESH_KEY", "valorant2024ranking"):
        raise HTTPException(status_code=403, detail="Chave inválida")
    
    # Verifica se o snapshot existe
    stmt = select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id)
    result = await db.execute(stmt)
    snapshot = result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} não encontrado")
    
    try:
        # Conta quantos registros históricos serão excluídos
        count_stmt = select(func.count(RankingHistory.id)).where(
            RankingHistory.snapshot_id == snapshot_id
        )
        count_result = await db.execute(count_stmt)
        history_count = count_result.scalar() or 0
        
        # Exclui os registros históricos
        delete_history_stmt = delete(RankingHistory).where(
            RankingHistory.snapshot_id == snapshot_id
        )
        await db.execute(delete_history_stmt)
        
        # Exclui o snapshot
        await db.delete(snapshot)
        await db.commit()
        
        # Limpa o cache do ranking se necessário
        ranking_cache["data"] = None
        ranking_cache["timestamp"] = None
        
        logger.info(f"Snapshot {snapshot_id} excluído com sucesso ({history_count} registros históricos)")
        
        return {
            "success": True,
            "message": f"Snapshot {snapshot_id} excluído com sucesso",
            "deleted_history_entries": history_count
        }
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Erro ao excluir snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao excluir snapshot: {str(e)}")

@app.get("/ranking/stats/summary", tags=["ranking"])
async def get_ranking_summary(db: AsyncSession = Depends(get_db)):
    """Retorna estatísticas gerais sobre o ranking"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    try:
        ranking_response = await get_ranking(db=db, force_refresh=False)
        ranking_data = ranking_response.get("ranking", [])
        
        if not ranking_data:
            return {"message": "Nenhum ranking disponível"}
        
        notas = [r.get("nota_final", 0) for r in ranking_data]
        games = [r.get("games_count", 0) for r in ranking_data]
        incertezas = [r.get("incerteza", 0) for r in ranking_data]
        
        # Distribuição por faixas de nota
        faixas = {
            "90+": sum(1 for n in notas if n >= 90),
            "80-89": sum(1 for n in notas if 80 <= n < 90),
            "70-79": sum(1 for n in notas if 70 <= n < 80),
            "60-69": sum(1 for n in notas if 60 <= n < 70),
            "50-59": sum(1 for n in notas if 50 <= n < 60),
            "<50": sum(1 for n in notas if n < 50)
        }
        
        return {
            "total_teams": len(ranking_data),
            "stats": {
                "nota_final": {
                    "max": max(notas) if notas else 0,
                    "min": min(notas) if notas else 0,
                    "mean": sum(notas) / len(notas) if notas else 0,
                    "median": sorted(notas)[len(notas)//2] if notas else 0,
                    "std_dev": (sum((x - sum(notas)/len(notas))**2 for x in notas) / len(notas))**0.5 if notas else 0
                },
                "games_count": {
                    "max": max(games) if games else 0,
                    "min": min(games) if games else 0,
                    "mean": sum(games) / len(games) if games else 0,
                    "total": sum(games) if games else 0
                },
                "incerteza": {
                    "max": max(incertezas) if incertezas else 0,
                    "min": min(incertezas) if incertezas else 0,
                    "mean": sum(incertezas) / len(incertezas) if incertezas else 0
                }
            },
            "distribution": faixas,
            "top_5": ranking_data[:5],
            "bottom_5": ranking_data[-5:] if len(ranking_data) > 5 else [],
            "last_update": ranking_response.get("last_update", ""),
            "cached": ranking_response.get("cached", False)
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")

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
    history_data = result.all()
    
    if not history_data:
        return {
            "team": {
                "id": team.id,
                "name": team.name,
                "tag": team.tag,
                "university": team.university,
                "logo": team.logo
            },
            "current_position": None,
            "position_change": 0,
            "nota_change": 0,
            "history": [],
            "message": "Time não possui histórico de ranking"
        }
    
    history = []
    for ranking, snapshot_date, total_teams in history_data:
        history.append({
            "snapshot_id": ranking.snapshot_id,
            "date": snapshot_date.isoformat(),
            "position": ranking.position,
            "total_teams": total_teams,
            "nota_final": ranking.nota_final,
            "games_count": ranking.games_count,
            "incerteza": ranking.incerteza,
            "percentile": round((1 - (ranking.position - 1) / total_teams) * 100, 1) if total_teams > 0 else 0
        })
    
    # Calcula variações
    if len(history) >= 2:
        position_change = history[1]["position"] - history[0]["position"]
        nota_change = history[0]["nota_final"] - history[1]["nota_final"]
    else:
        position_change = 0
        nota_change = 0
    
    return {
        "team": {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "university": team.university,
            "logo": team.logo
        },
        "current_position": history[0]["position"] if history else None,
        "position_change": position_change,
        "nota_change": round(nota_change, 3),
        "history": history
    }

@app.get("/ranking/{team_id}", tags=["ranking"])
async def get_team_ranking(team_id: int, db: AsyncSession = Depends(get_db)):
    """Retorna a posição e detalhes de ranking de um time específico"""
    if not RANKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Sistema de ranking não disponível")
    
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    try:
        ranking_response = await get_ranking(db=db, force_refresh=False)
        ranking_data = ranking_response.get("ranking", [])
        
        team_ranking = None
        for item in ranking_data:
            if item.get("team_id") == team_id:
                team_ranking = item
                break
        
        if not team_ranking:
            return {
                "team_id": team_id,
                "team_name": team.name,
                "message": "Time não possui partidas suficientes para aparecer no ranking"
            }
        
        # Adiciona informações do time
        team_ranking["team_details"] = {
            "university": team.university,
            "logo": team.logo
        }
        
        return team_ranking
        
    except Exception as e:
        logger.error(f"Erro ao buscar ranking do time {team_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar ranking: {str(e)}")

# ════════════════════════════════ DEBUG ════════════════════════════════

if os.getenv("ENVIRONMENT", "production") == "development":
    
    @app.get("/debug/tables", tags=["debug"])
    async def debug_tables(db: AsyncSession = Depends(get_db)):
        """[DEBUG] Lista todas as tabelas do banco"""
        try:
            result = await db.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            
            counts = {}
            for table in tables:
                try:
                    count_result = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    counts[table] = count_result.scalar()
                except:
                    counts[table] = "error"
            
            return {
                "tables": tables, 
                "record_counts": counts,
                "total_tables": len(tables)
            }
        except Exception as e:
            return {"error": str(e)}
    
    @app.get("/debug/test-ranking", tags=["debug"])
    async def debug_test_ranking(
        limit: int = Query(10, description="Número de times para mostrar"),
        db: AsyncSession = Depends(get_db)
    ):
        """[DEBUG] Testa o cálculo de ranking"""
        if not RANKING_AVAILABLE:
            return {"error": "Sistema de ranking não disponível"}
        
        try:
            ranking_data = await calculate_ranking(db)
            
            if not ranking_data:
                return {
                    "success": False,
                    "message": "Nenhum dado de ranking calculado",
                    "total_teams": 0
                }
            
            games_counts = [r["games_count"] for r in ranking_data]
            notas = [r["nota_final"] for r in ranking_data]
            
            return {
                "success": True,
                "total_teams": len(ranking_data),
                "stats": {
                    "games_count": {
                        "min": min(games_counts) if games_counts else 0,
                        "max": max(games_counts) if games_counts else 0,
                        "avg": sum(games_counts) / len(games_counts) if games_counts else 0
                    },
                    "nota_final": {
                        "min": min(notas) if notas else 0,
                        "max": max(notas) if notas else 0,
                        "avg": sum(notas) / len(notas) if notas else 0
                    }
                },
                "top_teams": ranking_data[:limit] if ranking_data else [],
                "bottom_teams": ranking_data[-5:] if len(ranking_data) > 5 else []
            }
        except Exception as e:
            import traceback
            return {
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
    
    @app.get("/debug/cache-status", tags=["debug"])
    async def debug_cache_status():
        """[DEBUG] Status do cache do ranking"""
        if not RANKING_AVAILABLE:
            return {"error": "Sistema de ranking não disponível"}
        
        now = datetime.now(timezone.utc)
        cache_age = None
        if ranking_cache["timestamp"]:
            cache_age = (now - ranking_cache["timestamp"]).total_seconds()
        
        return {
            "has_data": ranking_cache["data"] is not None,
            "data_count": len(ranking_cache["data"]) if ranking_cache["data"] else 0,
            "timestamp": ranking_cache["timestamp"].isoformat() if ranking_cache["timestamp"] else None,
            "cache_age_seconds": cache_age,
            "ttl_seconds": ranking_cache["ttl"].total_seconds(),
            "is_expired": cache_age > ranking_cache["ttl"].total_seconds() if cache_age else True
        }
    
@app.get("/debug/team/{team_id}/matches", tags=["debug"])
async def debug_team_matches(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Debug detalhado para problemas com partidas de times"""
    
    response = {
        "team_id": team_id,
        "team_exists": False,
        "team_info": None,
        "match_count": 0,
        "team_match_info_count": 0,
        "sample_tmi": None,
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