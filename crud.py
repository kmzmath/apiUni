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

async def get_team_stats(db: AsyncSession, team_id: int) -> dict[str, any]:
    """
    Retorna estatísticas de vitórias/derrotas de um time
    """
    try:
        # Query corrigida para contar vitórias e derrotas corretamente
        stmt = text("""
            WITH team_matches AS (
                SELECT 
                    m.id as match_id,
                    m.date,
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN tmi.score
                        ELSE tmi_b.score
                    END as team_score,
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN tmi_b.score
                        ELSE tmi.score
                    END as opponent_score,
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN t_b.name
                        ELSE t_a.name
                    END as opponent_name,
                    t.name as tournament_name
                FROM matches m
                JOIN team_match_info tmi ON (
                    (tmi.id = m.team_match_info_a OR tmi.id = m.team_match_info_b) 
                    AND tmi.team_id = :team_id
                )
                JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
                JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
                JOIN teams t_a ON tmi_a.team_id = t_a.id
                JOIN teams t_b ON tmi_b.team_id = t_b.id
                LEFT JOIN tournaments t ON m.tournament_id = t.id
            )
            SELECT 
                COUNT(*) as total_matches,
                COUNT(CASE WHEN team_score > opponent_score THEN 1 END) as wins,
                COUNT(CASE WHEN team_score < opponent_score THEN 1 END) as losses,
                COUNT(CASE WHEN team_score = opponent_score THEN 1 END) as draws,
                SUM(team_score) as total_rounds_won,
                SUM(opponent_score) as total_rounds_lost,
                AVG(team_score) as avg_rounds_won,
                AVG(opponent_score) as avg_rounds_lost
            FROM team_matches
        """)
        
        result = await db.execute(stmt, {"team_id": team_id})
        row = result.first()
        
        if not row or row.total_matches == 0:
            return {
                "total_matches": 0,
                "wins": 0,
                "losses": 0,
                "draws": 0,
                "win_rate": 0.0,
                "total_rounds_won": 0,
                "total_rounds_lost": 0,
                "avg_rounds_won": 0.0,
                "avg_rounds_lost": 0.0
            }
        
        win_rate = (row.wins / row.total_matches * 100) if row.total_matches > 0 else 0
        
        return {
            "total_matches": row.total_matches,
            "wins": row.wins,
            "losses": row.losses,
            "draws": row.draws,
            "win_rate": round(win_rate, 2),
            "total_rounds_won": row.total_rounds_won or 0,
            "total_rounds_lost": row.total_rounds_lost or 0,
            "avg_rounds_won": round(row.avg_rounds_won, 2) if row.avg_rounds_won else 0.0,
            "avg_rounds_lost": round(row.avg_rounds_lost, 2) if row.avg_rounds_lost else 0.0
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas do time {team_id}: {e}")
        return {
            "total_matches": 0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "win_rate": 0.0,
            "total_rounds_won": 0,
            "total_rounds_lost": 0,
            "avg_rounds_won": 0.0,
            "avg_rounds_lost": 0.0
        }

async def get_team_tournaments(db: AsyncSession, team_id: int):
    """Retorna todos os torneios que um time participou"""
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
            WHERE tmi.team_id = :team_id
            GROUP BY t.id, t.name, t.logo, t.organizer, t."startsOn", t."endsOn"
            ORDER BY MAX(m.date) DESC
        """)
        
        # IMPORTANTE: Passar o parâmetro team_id corretamente
        result = await db.execute(stmt, {"team_id": team_id})
        return result.all()
    except Exception as e:
        logger.error(f"Erro ao buscar torneios do time {team_id}: {e}")
        return []

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