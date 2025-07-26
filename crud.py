# crud.py
from __future__ import annotations

import uuid
import logging
from typing import List, Optional, Dict, Any

from sqlalchemy import select, text, func
from sqlalchemy.orm import Session, selectinload

from models import (
    Team,
    Tournament,
    Match,
    TeamMatchInfo,
    TeamPlayer,
    Estado,
)
import schemas

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Ranking (pode não existir no ambiente local)
try:
    from ranking import calculate_ranking

    RANKING_AVAILABLE = True
except ImportError:
    logger.warning("Sistema de ranking não disponível")

    def calculate_ranking(db, include_variation: bool = True):
        return []

    RANKING_AVAILABLE = False

# ════════════════════════════════ TEAMS ════════════════════════════════


def get_team(db: Session, team_id: int) -> Optional[Team]:
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.id == team_id)
    )
    result = db.execute(stmt)
    return result.scalar_one_or_none()


def list_teams(db: Session) -> List[Team]:
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .order_by(Team.name)
    )
    result = db.execute(stmt)
    return result.scalars().all()


def list_teams_minimal(db: Session) -> List[dict]:
    stmt = text(
        """
        SELECT
            t.id,
            t.name,
            t.tag,
            t.logo,
            e.sigla  AS estado_sigla,
            e.icone  AS estado_icone
        FROM teams t
        LEFT JOIN estados e ON t.estado_id = e.id
        ORDER BY t.name
    """
    )
    result = db.execute(stmt)
    return [
        dict(row._mapping)
        for row in result
    ]


def search_teams(
    db: Session,
    query: Optional[str] = None,
    org: Optional[str] = None,
    limit: int = 20,
) -> List[Team]:
    stmt = select(Team).options(selectinload(Team.estado_obj))
    
    if query:
        search = f"%{query}%"
        stmt = stmt.where(
            (Team.name.ilike(search)) |
            (Team.slug.ilike(search)) |
            (Team.tag.ilike(search))
        )
    
    if org:
        stmt = stmt.where(Team.org.ilike(f"%{org}%"))
    
    stmt = stmt.order_by(Team.name).limit(limit)
    result = db.execute(stmt)
    return result.scalars().all()


def get_team_matches(
    db: Session, team_id: int, limit: int = 50
) -> List[Match]:
    slug_subq = select(Team.slug).where(Team.id == team_id).scalar_subquery()

    stmt = (
        select(Match)
        .join(
            TeamMatchInfo,
            (Match.team_match_info_a == TeamMatchInfo.id) |
            (Match.team_match_info_b == TeamMatchInfo.id),
        )
        .where(TeamMatchInfo.team_slug == slug_subq)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date.desc())
        .limit(limit)
    )
    result = db.execute(stmt)
    return result.scalars().all()


def list_matches(db: Session, limit: int = 20) -> List[Match]:
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
    result = db.execute(stmt)
    matches = result.scalars().all()
    logger.info(f"Matches encontrados: {len(matches)}")
    return matches


def get_match(db: Session, match_id: uuid.UUID) -> Optional[Match]:
    stmt = (
        select(Match)
        .where(Match.id == match_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
    )
    result = db.execute(stmt)
    return result.scalar_one_or_none()

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════


def list_tournaments(db: Session) -> List[Tournament]:
    result = db.execute(
        select(Tournament).order_by(Tournament.start_date.desc())
    )
    return result.scalars().all()


def get_tournament(db: Session, tournament_id: uuid.UUID) -> Optional[Tournament]:
    result = db.execute(
        select(Tournament).where(Tournament.id == tournament_id)
    )
    return result.scalar_one_or_none()


def get_tournament_matches(db: Session, tournament_id: uuid.UUID) -> List[Match]:
    stmt = (
        select(Match)
        .where(Match.tournament_id == tournament_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date, Match.time)
    )
    result = db.execute(stmt)
    return result.scalars().all()

# ════════════════════════════════ STATS ════════════════════════════════


def get_maps_played(db: Session) -> List[Dict[str, Any]]:
    stmt = text("""
        SELECT 
            mapa as map_name,
            COUNT(*) as times_played
        FROM matches
        WHERE mapa IS NOT NULL
        GROUP BY mapa
        ORDER BY times_played DESC
    """)
    result = db.execute(stmt)
    return [{"map_name": row[0], "times_played": row[1]} for row in result]


def get_team_stats(db: Session, team_id: int) -> Dict[str, Any]:
    # Busca o slug do time
    team = get_team(db, team_id)
    if not team:
        return None
    
    # Estatísticas de vitórias/derrotas
    stmt = text("""
        WITH team_matches AS (
            SELECT 
                m.id,
                CASE 
                    WHEN tmi_a.team_slug = :team_slug THEN tmi_a.score
                    ELSE tmi_b.score
                END as team_score,
                CASE 
                    WHEN tmi_a.team_slug = :team_slug THEN tmi_b.score
                    ELSE tmi_a.score
                END as opponent_score
            FROM matches m
            JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
            JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
            WHERE tmi_a.team_slug = :team_slug OR tmi_b.team_slug = :team_slug
        )
        SELECT 
            COUNT(*) as total_matches,
            SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as losses,
            AVG(team_score) as avg_score,
            AVG(opponent_score) as avg_opponent_score
        FROM team_matches
    """)
    
    result = db.execute(stmt, {"team_slug": team.slug})
    stats = result.fetchone()
    
    if stats:
        return {
            "total_matches": stats[0] or 0,
            "wins": stats[1] or 0,
            "losses": stats[2] or 0,
            "winrate": round((stats[1] or 0) / (stats[0] or 1) * 100, 2),
            "avg_score": round(stats[3] or 0, 2),
            "avg_opponent_score": round(stats[4] or 0, 2)
        }
    
    return {
        "total_matches": 0,
        "wins": 0,
        "losses": 0,
        "winrate": 0,
        "avg_score": 0,
        "avg_opponent_score": 0
    }