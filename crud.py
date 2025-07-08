# crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload
from typing import List, Optional
import uuid
import logging

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer
import schemas

# Configurar logging
logger = logging.getLogger(__name__)

# ════════════════════════════════ TEAMS ════════════════════════════════

async def get_team(db: AsyncSession, team_id: int) -> Optional[schemas.Team]:
    """Busca um time pelo ID"""
    result = await db.execute(select(Team).where(Team.id == team_id))
    team = result.scalar_one_or_none()
    return team

async def list_teams(db: AsyncSession) -> List[schemas.Team]:
    """Lista todos os times ordenados por nome"""
    result = await db.execute(select(Team).order_by(Team.name))
    teams = result.scalars().all()
    return teams

async def search_teams(
    db: AsyncSession, 
    query: Optional[str] = None,
    university: Optional[str] = None,
    limit: int = 20
) -> List[schemas.Team]:
    """Busca times com filtros"""
    stmt = select(Team)
    
    if query:
        search_term = f"%{query}%"
        stmt = stmt.where(
            (Team.name.ilike(search_term)) |
            (Team.tag.ilike(search_term)) |
            (Team.slug.ilike(search_term))
        )
    
    if university:
        stmt = stmt.where(Team.university.ilike(f"%{university}%"))
    
    stmt = stmt.order_by(Team.name).limit(limit)
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

async def get_team_stats(db: AsyncSession, team_id: int) -> dict:
    """Retorna estatísticas de vitórias/derrotas de um time"""
    # Primeiro, busca estatísticas gerais
    match_stats_stmt = text("""
        WITH team_matches AS (
            SELECT 
                m.id,
                m.map,
                CASE 
                    WHEN m.team_match_info_a = tmi.id THEN tmi.score
                    ELSE tmi_b.score
                END as team_score,
                CASE 
                    WHEN m.team_match_info_a = tmi.id THEN tmi_b.score
                    ELSE tmi.score
                END as opponent_score
            FROM matches m
            JOIN team_match_info tmi ON (m.team_match_info_a = tmi.id OR m.team_match_info_b = tmi.id)
            JOIN team_match_info tmi_b ON (
                CASE 
                    WHEN m.team_match_info_a = tmi.id THEN m.team_match_info_b = tmi_b.id
                    ELSE m.team_match_info_a = tmi_b.id
                END
            )
            WHERE tmi.team_id = :team_id
        )
        SELECT 
            COUNT(DISTINCT id) as total_matches,
            COUNT(DISTINCT CASE WHEN team_score > opponent_score THEN id END) as wins,
            COUNT(DISTINCT CASE WHEN team_score < opponent_score THEN id END) as losses
        FROM team_matches
    """)
    
    # Busca estatísticas de mapas separadamente
    map_stats_stmt = text("""
        WITH team_matches AS (
            SELECT 
                m.id,
                m.map
            FROM matches m
            JOIN team_match_info tmi ON (m.team_match_info_a = tmi.id OR m.team_match_info_b = tmi.id)
            WHERE tmi.team_id = :team_id AND m.map IS NOT NULL
        )
        SELECT 
            map,
            COUNT(*) as map_count
        FROM team_matches
        GROUP BY map
        ORDER BY map_count DESC
    """)
    
    # Executa as queries
    match_result = await db.execute(match_stats_stmt, {"team_id": team_id})
    match_stats = match_result.first()
    
    map_result = await db.execute(map_stats_stmt, {"team_id": team_id})
    
    # Monta o dicionário de mapas
    maps_played = {}
    for row in map_result:
        maps_played[row.map] = row.map_count
    
    # Valores padrão se não houver dados
    total_matches = match_stats.total_matches if match_stats else 0
    wins = match_stats.wins if match_stats else 0
    losses = match_stats.losses if match_stats else 0
    
    win_rate = (wins / total_matches * 100) if total_matches > 0 else 0
    
    return {
        "total_matches": total_matches,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "maps_played": maps_played
    }

# NOVO: Função para buscar torneios de uma equipe
async def get_team_tournaments(db: AsyncSession, team_id: int):
    """Retorna todos os torneios que uma equipe participou com estatísticas"""
    try:
        stmt = text("""
            SELECT DISTINCT
                t.id,
                t.name,
                t.logo,
                t.organizer,
                t."startsOn" as starts_on,
                t."endsOn" as ends_on,
                COUNT(DISTINCT m.id) as matches_played,
                MIN(m.date) as first_match,
                MAX(m.date) as last_match,
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
            WHERE tmi.team_id = $1
            GROUP BY t.id, t.name, t.logo, t.organizer, t."startsOn", t."endsOn"
            ORDER BY MAX(m.date) DESC
        """)
        
        result = await db.execute(stmt, {"team_id": team_id})
        return result.all()
    except Exception as e:
        logger.error(f"Erro ao buscar torneios do time {team_id}: {e}")
        return []  # Return empty list instead of None

# ════════════════════════════════ MATCHES ════════════════════════════════

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

async def list_matches(db: AsyncSession, limit: int = 20) -> List[schemas.Match]:
    """Lista as partidas mais recentes"""
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
    return matches

async def get_match(db: AsyncSession, match_id: uuid.UUID) -> Optional[schemas.Match]:
    """Busca uma partida específica"""
    stmt = (
        select(Match)
        .where(Match.id == match_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
    )
    
    result = await db.execute(stmt)
    match = result.scalar_one_or_none()
    return match

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

async def list_tournaments(db: AsyncSession) -> List[schemas.Tournament]:
    """Lista todos os torneios"""
    result = await db.execute(
        select(Tournament).order_by(Tournament.starts_on.desc())
    )
    tournaments = result.scalars().all()
    return tournaments

async def get_tournament(
    db: AsyncSession, 
    tournament_id: uuid.UUID
) -> Optional[schemas.Tournament]:
    """Busca um torneio específico"""
    result = await db.execute(
        select(Tournament).where(Tournament.id == tournament_id)
    )
    tournament = result.scalar_one_or_none()
    return tournament

async def get_tournament_matches(
    db: AsyncSession, 
    tournament_id: uuid.UUID
) -> List[schemas.Match]:
    """Retorna todas as partidas de um torneio"""
    stmt = (
        select(Match)
        .where(Match.tournament_id == tournament_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date.desc())
    )
    
    result = await db.execute(stmt)
    matches = result.scalars().all()
    return matches

# ════════════════════════════════ STATS ════════════════════════════════

async def get_maps_played(db: AsyncSession) -> dict:
    """Retorna estatísticas de mapas jogados"""
    stmt = text("""
        SELECT 
            map,
            COUNT(*) as times_played,
            COUNT(DISTINCT DATE(date)) as days_played
        FROM matches
        WHERE map IS NOT NULL
        GROUP BY map
        ORDER BY times_played DESC
    """)
    
    result = await db.execute(stmt)
    maps = {}
    
    for row in result:
        maps[row.map] = {
            "times_played": row.times_played,
            "days_played": row.days_played
        }
    
    return {
        "maps": maps,
        "total_maps": len(maps)
    }