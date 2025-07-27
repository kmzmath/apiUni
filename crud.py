# crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload
from typing import List, Optional, Dict, Any
import uuid
import logging

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer, Estado
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
    """Retorna os jogadores de um time"""
    stmt = (
        select(TeamPlayer.player_nick, TeamPlayer.id)
        .where(TeamPlayer.team_id == team_id)
        .order_by(TeamPlayer.id)
    )
    result = await db.execute(stmt)
    players = [{"nick": row[0], "id": row[1]} for row in result]
    return players

async def get_team_matches(
    db: AsyncSession, 
    team_id: int, 
    limit: int = 50
) -> List[schemas.Match]:
    """Retorna todas as partidas de um time"""
    stmt = (
        select(Match)
        .join(TeamMatchInfo, (Match.team_match_info_a == TeamMatchInfo.id) | 
                            (Match.team_match_info_b == TeamMatchInfo.id))
        .where(TeamMatchInfo.team_id == team_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date.desc())
        .limit(limit)
    )
    
    result = await db.execute(stmt)
    matches = result.scalars().all()
    return matches

# ════════════════════════════════ MATCHES ════════════════════════════════

async def list_matches(db: AsyncSession, limit: int = 20) -> List[schemas.Match]:
    """Lista as partidas mais recentes"""
    try:
        stmt = (
            select(Match)
            .options(
                selectinload(Match.tournament),
                selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
                selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
            )
            .order_by(Match.date.desc())
            .limit(limit)
        )
        
        result = await db.execute(stmt)
        matches = result.scalars().all()
        
        logger.info(f"Matches encontradas: {len(matches)}")
        return matches
    except Exception as e:
        logger.error(f"Erro em list_matches: {str(e)}", exc_info=True)
        raise

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

async def list_tournaments(db: AsyncSession) -> List[schemas.Tournament]:
    """Lista todos os torneios"""
    result = await db.execute(
        select(Tournament).order_by(Tournament.starts_on.desc())
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
            t.university
        FROM ranking_history rh
        JOIN teams t ON rh.team_id = t.id
        WHERE rh.snapshot_id = :snapshot_id
        ORDER BY rh.position
    """)
    
    result = await db.execute(stmt, {"snapshot_id": snapshot_id})
    return result.all()