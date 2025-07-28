# main.py
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, Depends, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from database import get_db
from models import Team, Match, RankingSnapshot, RankingHistory, TeamPlayer
import crud
import schemas

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Função auxiliar para converter valores
def _f(v): 
    return float(v) if v is not None else None

# Configuração da API
app = FastAPI(
    title="Valorant Universitário API",
    version="2.0.0",
    docs_url="/docs",
    description="API para consultar dados de partidas do Valorant Universitário - Supabase Edition"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ════════════════════════════════ ROOT ════════════════════════════════

@app.get("/")
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "API Valorant Universitário - Supabase Edition",
        "version": "2.0.0",
        "docs": "/docs",
        "status": "online"
    }

@app.get("/health", response_class=PlainTextResponse)
@app.head("/health", response_class=PlainTextResponse, include_in_schema=False)
async def health_check():
    """Health check endpoint para monitoramento"""
    return PlainTextResponse("OK", status_code=200)

# ════════════════════════════════ TEAMS ════════════════════════════════

@app.get("/teams", response_model=List[schemas.Team])
async def list_teams(db: AsyncSession = Depends(get_db)):
    """Lista todos os times ordenados alfabeticamente"""
    return await crud.list_teams(db)

@app.get("/teams/by-slug/{slug}")
async def get_team_by_slug(
    slug: str,
    db: AsyncSession = Depends(get_db)
):
    """Busca um time pelo slug"""
    team = await crud.get_team_by_slug(db, slug)
    
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Formata resposta para manter compatibilidade
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
        "university": team.org,  # Usa org diretamente
        "university_tag": team.orgTag,  # Usa orgTag diretamente
        "estado": team.estado,
        "estado_info": estado_info,
        "instagram": team.instagram,
        "twitch": team.twitch
    }

@app.get("/teams/{team_id}")
async def get_team(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Busca um time pelo ID"""
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    return team

@app.get("/teams/{team_id}/players")
async def get_team_players(
    team_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Retorna os jogadores de um time específico"""
    
    # Verifica se o time existe
    team = await crud.get_team(db, team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Time não encontrado")
    
    # Busca os jogadores
    players = await crud.get_team_players(db, team_id)
    
    return players

@app.get("/teams/{team_id}/matches", response_model=List[schemas.Match])
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
        
        logger.info(f"Buscando partidas do time {team_id}: {team.name}")
        
        matches = await crud.get_team_matches(db, team_id, limit)
        logger.info(f"Encontradas {len(matches)} partidas para o time {team_id}")
        return matches
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro interno do servidor")

# ════════════════════════════════ MATCHES ════════════════════════════════

@app.get("/matches", response_model=List[schemas.Match])
async def list_matches(
    limit: int = Query(20, ge=1, le=100, description="Número de partidas a retornar"),
    db: AsyncSession = Depends(get_db),
):
    """Retorna as partidas mais recentes com informações completas"""
    return await crud.list_matches(db, limit=limit)

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

@app.get("/tournaments", response_model=List[schemas.Tournament])
async def list_tournaments(db: AsyncSession = Depends(get_db)):
    """Lista todos os torneios ordenados por data de início"""
    return await crud.list_tournaments(db)

# ════════════════════════════════ RANKING ════════════════════════════════

@app.get("/ranking")
async def get_ranking(
    limit: Optional[int] = Query(None, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Retorna o ranking do último snapshot salvo
    """
    try:
        # Busca o último snapshot
        latest_snapshot = await crud.get_latest_ranking_snapshot(db)
        
        if not latest_snapshot:
            return {
                "ranking": [],
                "total": 0,
                "limit": limit,
                "message": "Nenhum snapshot de ranking disponível",
                "snapshot_id": None,
                "snapshot_date": None,
                "cached": False,
                "last_update": None
            }
        
        # Busca os dados do ranking
        ranking_data = await get_snapshot_ranking_with_variations(db, latest_snapshot.id)
        
        if not ranking_data:
            logger.warning(f"Snapshot #{latest_snapshot.id} existe mas não tem dados")
            return {
                "ranking": [],
                "total": 0,
                "limit": limit,
                "snapshot_id": latest_snapshot.id,
                "snapshot_date": latest_snapshot.created_at.isoformat(),
                "cached": False,
                "last_update": latest_snapshot.created_at.isoformat(),
                "message": "Snapshot existe mas não contém dados de ranking"
            }
        
        total_teams = len(ranking_data)
        
        if limit is not None:
            ranking_data = ranking_data[:limit]
        
        return {
            "ranking": ranking_data,
            "total": total_teams,
            "limit": limit,
            "snapshot_id": latest_snapshot.id,
            "snapshot_date": latest_snapshot.created_at.isoformat(),
            "cached": False,
            "last_update": latest_snapshot.created_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro no endpoint /ranking: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar ranking: {str(e)}"
        )

@app.post("/ranking/calculate")
async def calculate_ranking(
    admin_key: str = Query(..., description="Chave de administração"),
    db: AsyncSession = Depends(get_db)
):
    """
    Calcula um novo ranking baseado nos dados atuais e salva como snapshot.
    Requer chave de administração.
    """
    # Verificar chave de admin
    expected_key = os.getenv("ADMIN_KEY", "valorant2024admin")
    if admin_key != expected_key:
        raise HTTPException(status_code=403, detail="Chave de administração inválida")
    
    try:
        # Importar a calculadora
        from ranking_calculator import calculate_and_save_ranking
        
        # Calcular e salvar ranking
        logger.info("Iniciando cálculo de ranking...")
        snapshot = await calculate_and_save_ranking(db)
        
        return {
            "success": True,
            "snapshot_id": snapshot.id,
            "total_teams": snapshot.total_teams,
            "total_matches": snapshot.total_matches,
            "created_at": snapshot.created_at.isoformat(),
            "message": "Ranking calculado e salvo com sucesso"
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular ranking: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao calcular ranking: {str(e)}"
        )

@app.get("/ranking/live")
async def get_live_ranking(
    limit: Optional[int] = Query(None, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Calcula o ranking em tempo real (sem salvar no banco).
    Útil para testes e validação.
    """
    try:
        from ranking_calculator import RankingCalculator
        from sqlalchemy.orm import selectinload
        
        # Buscar todos os times
        teams_result = await db.execute(select(Team))
        teams = teams_result.scalars().all()
        
        # Buscar todas as partidas com relacionamentos
        # Match já está importado no topo, então pode usar diretamente
        matches_result = await db.execute(
            select(Match)
            .options(
                selectinload(Match.team_a),
                selectinload(Match.team_b),
                selectinload(Match.tournament),
                selectinload(Match.map_obj)
            )
            .order_by(Match.date, Match.time)
        )
        matches = matches_result.scalars().all()
        
        logger.info(f"Calculando ranking ao vivo com {len(teams)} times e {len(matches)} partidas")
        
        # Calcular ranking
        calculator = RankingCalculator(teams, matches)
        ranking_df = calculator.calculate_final_ranking()
        
        # Criar mapa de slug para informações do time
        team_info = {team.slug: {
            "id": team.id,
            "name": team.name,
            "tag": team.tag,
            "org": team.org
        } for team in teams}
        
        # Preparar resultado
        ranking_data = []
        for _, row in ranking_df.iterrows():
            team_data = team_info.get(row['team'], {})
            if not team_data:
                continue
                
            ranking_item = {
                "posicao": int(row['position']),
                "team_id": team_data.get('id'),
                "team": team_data.get('name', row['team']),
                "tag": team_data.get('tag'),
                "university": team_data.get('org'),
                "nota_final": float(row['NOTA_FINAL']),
                "ci_lower": float(row['ci_lower']),
                "ci_upper": float(row['ci_upper']),
                "incerteza": float(row['incerteza']),
                "games_count": int(row['games_count']),
                "scores": {
                    "colley": float(row['r_colley']),
                    "massey": float(row['r_massey']),
                    "elo": float(row['r_elo_final']),
                    "elo_mov": float(row['r_elo_mov']),
                    "trueskill": float(row['ts_score']),
                    "pagerank": float(row['r_pagerank']),
                    "bradley_terry": float(row['r_bt_pois']),
                    "pca": float(row['pca_score']),
                    "sos": float(row['sos_score']),
                    "consistency": float(row['consistency']),
                    "integrado": float(row['rating_integrado']),
                    "borda": float(row.get('score_borda', 0))
                }
            }
            ranking_data.append(ranking_item)
        
        if limit:
            ranking_data = ranking_data[:limit]
        
        return {
            "ranking": ranking_data,
            "total": len(ranking_df),
            "limit": limit,
            "live": True,
            "message": "Ranking calculado em tempo real (não salvo no banco)"
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular ranking ao vivo: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao calcular ranking: {str(e)}"
        )

@app.get("/ranking/snapshots")
async def list_snapshots(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Lista todos os snapshots disponíveis
    """
    try:
        snapshots = await crud.get_ranking_snapshots(db, limit)
        
        snapshots_data = []
        for snapshot in snapshots:
            snapshots_data.append({
                "id": snapshot.id,
                "created_at": snapshot.created_at.isoformat(),
                "total_teams": snapshot.total_teams,
                "total_matches": snapshot.total_matches,
                "metadata": snapshot.snapshot_metadata
            })
        
        return {
            "data": snapshots_data,
            "count": len(snapshots)
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar snapshots: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao listar snapshots: {str(e)}"
        )

# ════════════════════════════════ FUNÇÕES AUXILIARES ════════════════════════════════

async def get_snapshot_ranking_with_variations(
    db: AsyncSession, 
    snapshot_id: int
) -> List[Dict[str, Any]]:
    """
    Retorna o ranking de um snapshot específico com variações
    calculadas em relação ao snapshot anterior.
    """
    try:
        # Busca dados do ranking atual
        ranking_rows = await crud.get_ranking_data_from_snapshot(db, snapshot_id)
        
        current_ranking = {}
        for row in ranking_rows:
            current_ranking[row.team_id] = {
                "row": row,
                "position": row.position,
                "nota_final": float(row.nota_final)
            }
        
        if not current_ranking:
            logger.warning(f"Snapshot #{snapshot_id} não possui dados de ranking")
            return []
        
        # Busca o snapshot anterior
        prev_snapshot = await db.scalar(
            select(RankingSnapshot)
            .where(RankingSnapshot.id < snapshot_id)
            .order_by(RankingSnapshot.id.desc())
            .limit(1)
        )
        
        previous_data = {}
        if prev_snapshot:
            # Busca dados do snapshot anterior
            prev_history = await db.execute(
                select(RankingHistory)
                .where(RankingHistory.snapshot_id == prev_snapshot.id)
            )
            
            for entry in prev_history.scalars():
                previous_data[entry.team_id] = {
                    'position': entry.position,
                    'nota_final': float(entry.nota_final)
                }
        
        # Monta o resultado com variações
        ranking_data = []
        
        for team_id, current in current_ranking.items():
            row = current["row"]
            
            # Calcula variações se houver dados anteriores
            variacao = None
            variacao_nota = None
            is_new = False
            
            if team_id in previous_data:
                variacao = previous_data[team_id]['position'] - current['position']
                variacao_nota = round(current['nota_final'] - previous_data[team_id]['nota_final'], 2)
            elif prev_snapshot:
                is_new = True
            
            ranking_item = {
                "posicao": row.position,
                "team_id": row.team_id,
                "team": row.name,
                "tag": row.tag,
                "university": row.university,
                "nota_final": float(row.nota_final),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": row.games_count,
                "variacao": variacao,
                "variacao_nota": variacao_nota,
                "is_new": is_new,
                "scores": {
                    "colley": _f(row.score_colley),
                    "massey": _f(row.score_massey),
                    "elo": _f(row.score_elo_final),
                    "elo_mov": _f(row.score_elo_mov),
                    "trueskill": _f(row.score_trueskill),
                    "pagerank": _f(row.score_pagerank),
                    "bradley_terry": _f(row.score_bradley_terry),
                    "pca": _f(row.score_pca),
                    "sos": _f(row.score_sos),
                    "consistency": _f(row.score_consistency),
                    "integrado": _f(row.score_integrado),
                    "borda": row.score_borda if hasattr(row, 'score_borda') else None
                }
            }
            
            ranking_data.append(ranking_item)
        
        # Ordena por posição
        ranking_data.sort(key=lambda x: x["posicao"])
        
        return ranking_data
        
    except Exception as e:
        logger.error(f"Erro ao buscar ranking com variações para snapshot #{snapshot_id}: {str(e)}")
        raise