# crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload, joinedload
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer, Estado, Map
import schemas

# Configurar logging
logger = logging.getLogger(__name__)

# ════════════════════════════════ TEAMS ════════════════════════════════

async def get_team(db: AsyncSession, team_id: int) -> Optional[schemas.Team]:
    """Busca um time pelo ID"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.id == team_id)
    )
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    return team

async def get_team_by_slug(db: AsyncSession, slug: str) -> Optional[Team]:
    """Busca um time pelo slug"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.slug == slug)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def list_teams(db: AsyncSession) -> List[schemas.Team]:
    """Lista todos os times"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .order_by(Team.name)
    )
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

async def get_team_players(db: AsyncSession, team_id: int) -> List[Dict[str, Any]]:
    """Retorna os jogadores de um time das colunas player1-player10"""
    team = await get_team(db, team_id)
    if not team:
        return []
    
    players = []
    # Pega os jogadores das colunas player1 até player10
    for i in range(1, 11):
        player_nick = getattr(team, f'player{i}', None)
        if player_nick:
            players.append({
                "nick": player_nick,
                "id": i  # Usa o número da posição como ID
            })
    
    return players

async def get_team_matches(
    db: AsyncSession, 
    team_id: int, 
    limit: int = 50
) -> List[schemas.Match]:
    """Retorna todas as partidas de um time"""
    # Primeiro busca o slug do time
    team = await get_team(db, team_id)
    if not team:
        return []
    
    # Busca matches onde o time aparece (por slug)
    stmt = text("""
        SELECT 
            m."idPartida",
            m.date,
            m.time,
            m.team_i,
            m.team_j,
            m.score_i,
            m.score_j,
            m.campeonato,
            m.fase,
            m.mapa,
            t_i.id as team_i_id,
            t_i.name as team_i_name,
            t_i.tag as team_i_tag,
            t_i.logo as team_i_logo,
            t_j.id as team_j_id,
            t_j.name as team_j_name,
            t_j.tag as team_j_tag,
            t_j.logo as team_j_logo,
            tour.id as tournament_id,
            tour.name as tournament_name,
            tour.logo as tournament_logo,
            tour.organizer as tournament_organizer,
            mp.nome_mapa as map_name
        FROM matches m
        LEFT JOIN teams t_i ON m.team_i = t_i.slug
        LEFT JOIN teams t_j ON m.team_j = t_j.slug
        LEFT JOIN tournaments tour ON m.campeonato = tour.name
        LEFT JOIN maps mp ON m.mapa = mp.slug
        WHERE t_i.id = :team_id OR t_j.id = :team_id
        ORDER BY m.date DESC, m.time DESC
        LIMIT :limit
    """)
    
    result = await db.execute(stmt, {"team_id": team_id, "limit": limit})
    rows = result.fetchall()
    
    matches = []
    for row in rows:
        # Cria objetos simulados para compatibilidade
        match_dict = {
            "id": row.idPartida,
            "date": datetime.combine(row.date, row.time).replace(tzinfo=timezone.utc),
            "map": row.map_name or row.mapa,
            "round": row.fase,
            "tournament": {
                "id": row.tournament_id,
                "name": row.tournament_name,
                "logo": row.tournament_logo,
                "organizer": row.tournament_organizer,
                "startsOn": None,
                "endsOn": None
            } if row.tournament_name else None,
            "tmi_a": {
                "id": None,
                "team": {
                    "id": row.team_i_id,
                    "name": row.team_i_name,
                    "tag": row.team_i_tag,
                    "logo": row.team_i_logo
                },
                "score": row.score_i
            },
            "tmi_b": {
                "id": None,
                "team": {
                    "id": row.team_j_id,
                    "name": row.team_j_name,
                    "tag": row.team_j_tag,
                    "logo": row.team_j_logo
                },
                "score": row.score_j
            }
        }
        
        # Converte para objeto Match simulado
        class FakeMatch:
            def __init__(self, data):
                self.__dict__.update(data)
                
        matches.append(FakeMatch(match_dict))
    
    return matches

# ════════════════════════════════ MATCHES ════════════════════════════════

async def list_matches(db: AsyncSession, limit: int = 20) -> List[schemas.Match]:
    """Lista as partidas mais recentes"""
    try:
        stmt = text("""
            SELECT 
                m."idPartida",
                m.date,
                m.time,
                m.team_i,
                m.team_j,
                m.score_i,
                m.score_j,
                m.campeonato,
                m.fase,
                m.mapa,
                t_i.id as team_i_id,
                t_i.name as team_i_name,
                t_i.tag as team_i_tag,
                t_i.logo as team_i_logo,
                t_j.id as team_j_id,
                t_j.name as team_j_name,
                t_j.tag as team_j_tag,
                t_j.logo as team_j_logo,
                tour.id as tournament_id,
                tour.name as tournament_name,
                tour.logo as tournament_logo,
                tour.organizer as tournament_organizer,
                mp.nome_mapa as map_name
            FROM matches m
            LEFT JOIN teams t_i ON m.team_i = t_i.slug
            LEFT JOIN teams t_j ON m.team_j = t_j.slug
            LEFT JOIN tournaments tour ON m.campeonato = tour.name
            LEFT JOIN maps mp ON m.mapa = mp.slug
            ORDER BY m.date DESC, m.time DESC
            LIMIT :limit
        """)
        
        result = await db.execute(stmt, {"limit": limit})
        rows = result.fetchall()
        
        matches = []
        for row in rows:
            # Cria objetos simulados para compatibilidade
            match_dict = {
                "id": row.idPartida,
                "date": datetime.combine(row.date, row.time).replace(tzinfo=timezone.utc),
                "map": row.map_name or row.mapa,
                "round": row.fase,
                "tournament": {
                    "id": row.tournament_id,
                    "name": row.tournament_name,
                    "logo": row.tournament_logo,
                    "organizer": row.tournament_organizer,
                    "startsOn": None,
                    "endsOn": None
                } if row.tournament_name else None,
                "tmi_a": {
                    "id": None,
                    "team": {
                        "id": row.team_i_id,
                        "name": row.team_i_name,
                        "tag": row.team_i_tag,
                        "logo": row.team_i_logo
                    },
                    "score": row.score_i
                },
                "tmi_b": {
                    "id": None,
                    "team": {
                        "id": row.team_j_id,
                        "name": row.team_j_name,
                        "tag": row.team_j_tag,
                        "logo": row.team_j_logo
                    },
                    "score": row.score_j
                }
            }
            
            # Converte para objeto Match simulado
            class FakeMatch:
                def __init__(self, data):
                    self.__dict__.update(data)
                    
            matches.append(FakeMatch(match_dict))
        
        logger.info(f"Matches encontradas: {len(matches)}")
        return matches
        
    except Exception as e:
        logger.error(f"Erro em list_matches: {str(e)}", exc_info=True)
        raise

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

async def list_tournaments(db: AsyncSession) -> List[schemas.Tournament]:
    """Lista todos os torneios"""
    result = await db.execute(
        select(Tournament).order_by(Tournament.start_date.desc().nullslast())
    )
    tournaments = result.scalars().all()
    return tournaments

# ════════════════════════════════ RANKING ════════════════════════════════

async def get_latest_ranking_snapshot(db: AsyncSession) -> Optional[Any]:
    """Retorna o último snapshot de ranking"""
    from models import RankingSnapshot
    
    stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(1)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_ranking_snapshots(db: AsyncSession, limit: int = 20) -> List[Any]:
    """Lista os snapshots de ranking"""
    from models import RankingSnapshot
    
    stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_ranking_data_from_snapshot(db: AsyncSession, snapshot_id: int) -> List[Dict[str, Any]]:
    """Busca dados de ranking de um snapshot específico"""
    stmt = text("""
        SELECT 
            rh.*,
            t.name,
            t.tag,
            t.org as university
        FROM ranking_history rh
        JOIN teams t ON rh.team_id = t.id
        WHERE rh.snapshot_id = :snapshot_id
        ORDER BY rh.position
    """)
    
    result = await db.execute(stmt, {"snapshot_id": snapshot_id})
    return result.all()