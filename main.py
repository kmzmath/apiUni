import os
import asyncio
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import logging
from functools import wraps

from fastapi import FastAPI, Depends, HTTPException, Query, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Team, Estado, TeamPlayer, Tournament, Match, TeamMatchInfo, RankingSnapshot, RankingHistory
import crud
import schemas

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração específica para produção
IS_PRODUCTION = os.getenv("RENDER") is not None
PORT = int(os.getenv("PORT", 8000))

# Configuração da API
app = FastAPI(
    title="Valorant Universitário API",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    description="API para consultar dados de partidas do Valorant Universitário"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001", 
        "https://univlr.com",
        "https://www.univlr.com",
        "https://univlr-web.vercel.app",
        "*"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

# Exception Handler Global
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Erro não tratado: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc) if not IS_PRODUCTION else "An error occurred"
        }
    )

# ===== HELPER FUNCTIONS =====

def format_team_dict(team: Team) -> dict:
    """
    Formata um objeto Team para o formato esperado pelo front-end
    IMPORTANTE: Mapeia 'org' -> 'university' e 'orgTag' -> 'university_tag'
    """
    estado_info = None
    if team.estado_obj:
        estado_info = {
            "id": team.estado_obj.id,
            "sigla": team.estado_obj.sigla,
            "nome": team.estado_obj.nome,
            "icone": team.estado_obj.icone or "",
            "regiao": team.estado_obj.regiao
        }
    
    return {
        "id": team.id,
        "name": team.name,
        "logo": team.logo or "",
        "tag": team.tag or "",
        "slug": team.slug or "",
        "university": team.org or "",  # MAPEAMENTO CRÍTICO
        "university_tag": team.orgTag or "",  # MAPEAMENTO CRÍTICO
        "estado": team.estado or "",
        "estado_info": estado_info,
        "instagram": team.instagram or "",
        "twitch": team.twitch or ""
    }

def format_match_dict(match: Match) -> dict:
    """Formata uma partida para o formato esperado pelo front-end"""
    # Formatar times
    team_a = None
    team_b = None
    
    # Usar team_match_info se disponível, senão usar teams diretos
    if match.tmi_a_rel and match.tmi_a_rel.team:
        team_a = format_team_dict(match.tmi_a_rel.team)
    elif match.team_i_obj:
        team_a = format_team_dict(match.team_i_obj)
    
    if match.tmi_b_rel and match.tmi_b_rel.team:
        team_b = format_team_dict(match.tmi_b_rel.team)
    elif match.team_j_obj:
        team_b = format_team_dict(match.team_j_obj)
    
    # Formatar torneio
    tournament = None
    if match.tournament_rel:
        tournament = {
            "id": match.tournament_rel.id,
            "name": match.tournament_rel.name,
            "logo": match.tournament_rel.logo or "",
            "organizer": match.tournament_rel.organizer or "",
            "startsOn": match.tournament_rel.start_date.isoformat() if match.tournament_rel.start_date else None,
            "endsOn": match.tournament_rel.end_date.isoformat() if match.tournament_rel.end_date else None
        }
    
    # Combinar data e hora
    match_datetime = datetime.combine(match.date, match.time)
    
    return {
        "id": match.idPartida,
        "map": match.mapa or "",
        "round": match.fase or "",
        "date": match_datetime.isoformat(),
        "tmi_a": {
            "id": str(match.tmi_a) if match.tmi_a else f"{match.idPartida}_a",
            "team": team_a,
            "score": match.tmi_a_rel.score if match.tmi_a_rel else match.score_i,
            "agent_1": match.tmi_a_rel.agent1 if match.tmi_a_rel else "",
            "agent_2": match.tmi_a_rel.agent2 if match.tmi_a_rel else "",
            "agent_3": match.tmi_a_rel.agent3 if match.tmi_a_rel else "",
            "agent_4": match.tmi_a_rel.agent4 if match.tmi_a_rel else "",
            "agent_5": match.tmi_a_rel.agent5 if match.tmi_a_rel else ""
        },
        "tmi_b": {
            "id": str(match.tmi_b) if match.tmi_b else f"{match.idPartida}_b",
            "team": team_b,
            "score": match.tmi_b_rel.score if match.tmi_b_rel else match.score_j,
            "agent_1": match.tmi_b_rel.agent1 if match.tmi_b_rel else "",
            "agent_2": match.tmi_b_rel.agent2 if match.tmi_b_rel else "",
            "agent_3": match.tmi_b_rel.agent3 if match.tmi_b_rel else "",
            "agent_4": match.tmi_b_rel.agent4 if match.tmi_b_rel else "",
            "agent_5": match.tmi_b_rel.agent5 if match.tmi_b_rel else ""
        },
        "tournament": tournament
    }

# ===== ROOT E HEALTH =====

@app.get("/")
async def root():
    return {
        "message": "API Valorant Universitário",
        "version": "2.0.0",
        "docs": "/docs",
        "status": "online"
    }

@app.get("/health")
async def health_check():
    return JSONResponse(
        content={
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

# ===== TEAMS ENDPOINTS =====

@app.get("/teams", response_model=List[schemas.Team])
async def list_teams(db: AsyncSession = Depends(get_db)):
    """
    Lista todos os times
    CRÍTICO: Retorna um ARRAY direto, não um objeto
    """
    try:
        teams = await crud.list_teams(db)
        
        # Formatar cada time para o formato esperado
        teams_list = [format_team_dict(team) for team in teams]
        
        logger.info(f"Retornando {len(teams_list)} times")
        return teams_list  # ARRAY DIRETO!
        
    except Exception as e:
        logger.error(f"Erro ao listar times: {str(e)}", exc_info=True)
        return []  # Retornar array vazio em caso de erro

@app.get("/teams/by-slug/{slug}", response_model=schemas.Team)
async def get_team_by_slug(
    slug: str,
    db: AsyncSession = Depends(get_db)
):
    """Busca um time pelo slug"""
    team = await crud.get_team_by_slug(db, slug)
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    return format_team_dict(team)

@app.get("/teams/{team_id}", response_model=schemas.Team)
async def get_team(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Busca um time pelo ID"""
    team = await crud.get_team(db, team_id)
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    return format_team_dict(team)

# ===== PLAYERS ENDPOINT =====

@app.get("/teams/{team_id}/players", response_model=List[schemas.Player])
async def get_team_players(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna os jogadores de um time"""
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    players = await crud.get_team_players(db, team_id)
    
    return [
        {"id": player.id, "nick": player.player_nick}
        for player in players
    ]

# ===== MATCHES ENDPOINTS =====

@app.get("/teams/{team_id}/matches", response_model=List[schemas.Match])
async def get_team_matches(
    team_id: int,
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Retorna as partidas de um time"""
    try:
        team = await crud.get_team(db, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Time não encontrado")
        
        matches = await crud.get_team_matches(db, team_id, limit)
        
        return [format_match_dict(match) for match in matches]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar partidas: {str(e)}", exc_info=True)
        return []

@app.get("/matches", response_model=List[schemas.Match])
async def list_matches(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Lista as partidas mais recentes"""
    try:
        matches = await crud.list_recent_matches(db, limit)
        return [format_match_dict(match) for match in matches]
    except Exception as e:
        logger.error(f"Erro ao listar partidas: {str(e)}", exc_info=True)
        return []

# ===== RANKING ENDPOINTS =====

@app.get("/ranking", response_model=schemas.RankingResponse)
async def get_ranking(
    limit: Optional[int] = Query(None, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Retorna o ranking atual"""
    try:
        # Buscar último snapshot
        snapshot = await crud.get_latest_ranking_snapshot(db)
        
        if not snapshot:
            return {
                "cached": False,
                "last_update": datetime.now().isoformat(),
                "limit": limit,
                "total": 0,
                "ranking": []
            }
        
        # Buscar ranking do snapshot
        rankings = await crud.get_ranking_by_snapshot(db, snapshot.id, limit)
        
        # Formatar ranking
        ranking_list = []
        for rank in rankings:
            ranking_list.append({
                "posicao": rank.position,
                "team_id": rank.team_id,
                "team": rank.team.name,
                "tag": rank.team.tag or "",
                "university": rank.team.org or "",  # Mapear org -> university
                "nota_final": float(rank.nota_final),
                "ci_lower": float(rank.ci_lower),
                "ci_upper": float(rank.ci_upper),
                "incerteza": float(rank.incerteza),
                "games_count": rank.games_count,
                "variacao": 0,
                "variacao_nota": 0.0,
                "is_new": False,
                "scores": {
                    "colley": float(rank.score_colley or 0),
                    "massey": float(rank.score_massey or 0),
                    "elo": float(rank.score_elo_final or 0),
                    "elo_mov": float(rank.score_elo_mov or 0),
                    "trueskill": float(rank.score_trueskill or 0),
                    "pagerank": float(rank.score_pagerank or 0),
                    "bradley_terry": float(rank.score_bradley_terry or 0),
                    "pca": float(rank.score_pca or 0),
                    "sos": float(rank.score_sos or 0),
                    "consistency": float(rank.score_consistency or 0),
                    "integrado": float(rank.score_integrado or 0)
                }
            })
        
        return {
            "cached": False,
            "last_update": snapshot.created_at.isoformat(),
            "limit": limit,
            "total": len(ranking_list),
            "ranking": ranking_list
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar ranking: {str(e)}", exc_info=True)
        return {
            "cached": False,
            "last_update": datetime.now().isoformat(),
            "limit": limit,
            "total": 0,
            "ranking": []
        }

@app.get("/ranking/snapshots", response_model=schemas.RankingSnapshotsResponse)
async def get_ranking_snapshots(
    limit: int = Query(10, ge=1, le=50),
    include_full_data: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna snapshots do ranking
    IMPORTANTE: Retorna objeto com propriedade 'data'
    """
    try:
        snapshots = await crud.get_ranking_snapshots(db, limit)
        
        snapshots_data = []
        for snapshot in snapshots:
            snapshot_dict = {
                "id": snapshot.id,
                "created_at": snapshot.created_at.isoformat(),
                "total_teams": snapshot.total_teams,
                "total_matches": snapshot.total_matches,
                "metadata": snapshot.snapshot_metadata or {
                    "version": "1.0",
                    "timestamp": snapshot.created_at.isoformat(),
                    "algorithms_used": ["elo", "trueskill", "colley", "massey", "pagerank"]
                },
                "ranking": []
            }
            
            if include_full_data:
                # Buscar ranking do snapshot
                rankings = await crud.get_ranking_by_snapshot(db, snapshot.id)
                
                for rank in rankings:
                    snapshot_dict["ranking"].append({
                        "posicao": rank.position,
                        "team_id": rank.team_id,
                        "team": rank.team.name,
                        "tag": rank.team.tag or "",
                        "university": rank.team.org or "",
                        "nota_final": float(rank.nota_final),
                        "ci_lower": float(rank.ci_lower),
                        "ci_upper": float(rank.ci_upper),
                        "incerteza": float(rank.incerteza),
                        "games_count": rank.games_count,
                        "variacao": 0,
                        "variacao_nota": 0.0,
                        "is_new": False,
                        "scores": {
                            "colley": float(rank.score_colley or 0),
                            "massey": float(rank.score_massey or 0),
                            "elo": float(rank.score_elo_final or 0),
                            "elo_mov": float(rank.score_elo_mov or 0),
                            "trueskill": float(rank.score_trueskill or 0),
                            "pagerank": float(rank.score_pagerank or 0),
                            "bradley_terry": float(rank.score_bradley_terry or 0),
                            "pca": float(rank.score_pca or 0),
                            "sos": float(rank.score_sos or 0),
                            "consistency": float(rank.score_consistency or 0),
                            "integrado": float(rank.score_integrado or 0)
                        }
                    })
            
            snapshots_data.append(snapshot_dict)
        
        # IMPORTANTE: Retornar com propriedade 'data'
        return {"data": snapshots_data}
        
    except Exception as e:
        logger.error(f"Erro ao buscar snapshots: {str(e)}", exc_info=True)
        return {"data": []}

# ===== TOURNAMENTS ENDPOINT =====

@app.get("/tournaments", response_model=List[schemas.Tournament])
async def list_tournaments(db: AsyncSession = Depends(get_db)):
    """Lista todos os torneios"""
    try:
        tournaments = await crud.list_tournaments(db)
        
        return [
            {
                "id": t.id,
                "name": t.name,
                "logo": t.logo or "",
                "organizer": t.organizer or "",
                "startsOn": t.start_date.isoformat() if t.start_date else None,
                "endsOn": t.end_date.isoformat() if t.end_date else None
            }
            for t in tournaments
        ]
        
    except Exception as e:
        logger.error(f"Erro ao listar torneios: {str(e)}", exc_info=True)
        return []

# Executar servidor se for o arquivo principal
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        reload=not IS_PRODUCTION
    )