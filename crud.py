# crud.py
from __future__ import annotations

import uuid
import logging
from typing import List, Optional, Dict, Any

from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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

    async def calculate_ranking(db, include_variation: bool = True):
        return []

    RANKING_AVAILABLE = False

# ════════════════════════════════ TEAMS ════════════════════════════════


async def get_team(db: AsyncSession, team_id: int) -> Optional[Team]:
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.id == team_id)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def list_teams(db: AsyncSession) -> List[Team]:
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .order_by(Team.name)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


async def list_teams_minimal(db: AsyncSession) -> List[dict]:
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
    result = await db.execute(stmt)
    return [
        dict(row._mapping)
        for row in result
    ]


async def search_teams(
    db: AsyncSession,
    query: Optional[str] = None,
    org: Optional[str] = None,
    limit: int = 20,
) -> List[Team]:
    stmt = select(Team).options(selectinload(Team.estado_obj))

    if query:
        like = f"%{query}%"
        stmt = stmt.where(
            (Team.name.ilike(like))
            | (Team.tag.ilike(like))
            | (Team.slug.ilike(like))
        )

    if org:
        stmt = stmt.where(Team.org.ilike(f"%{org}%"))

    stmt = stmt.order_by(Team.name).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()

# ════════════════════════════════ TEAM STATS ════════════════════════════════


async def get_team_stats(db: AsyncSession, team_id: int) -> Dict[str, Any]:
    # slug do time para usar em team_match_info
    slug = (
        await db.execute(select(Team.slug).where(Team.id == team_id))
    ).scalar()
    if not slug:
        return {
            "total_matches": 0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "win_rate": 0.0,
            "total_rounds_won": 0,
            "total_rounds_lost": 0,
            "avg_rounds_won": 0.0,
            "avg_rounds_lost": 0.0,
        }

    stmt = text(
        """
        WITH team_matches AS (
            SELECT
                m.id,
                CASE
                    WHEN tmi.id = m.team_match_info_a THEN tmi_a.score
                    ELSE tmi_b.score
                END AS team_score,
                CASE
                    WHEN tmi.id = m.team_match_info_a THEN tmi_b.score
                    ELSE tmi_a.score
                END AS opponent_score
            FROM matches            m
            JOIN team_match_info    tmi ON (
                 (tmi.id = m.team_match_info_a OR tmi.id = m.team_match_info_b)
                 AND tmi.team_slug = :team_slug
            )
            JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
            JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
        )
        SELECT
            COUNT(*)                                                     AS total_matches,
            COUNT(CASE WHEN team_score > opponent_score THEN 1 END)      AS wins,
            COUNT(CASE WHEN team_score < opponent_score THEN 1 END)      AS losses,
            COUNT(CASE WHEN team_score = opponent_score THEN 1 END)      AS draws,
            SUM(team_score)                                              AS total_rounds_won,
            SUM(opponent_score)                                          AS total_rounds_lost,
            AVG(team_score)                                              AS avg_rounds_won,
            AVG(opponent_score)                                          AS avg_rounds_lost
        FROM team_matches
    """
    )

    row = (await db.execute(stmt, {"team_slug": slug})).first()

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
            "avg_rounds_lost": 0.0,
        }

    win_rate = row.wins / row.total_matches * 100
    return {
        "total_matches": row.total_matches,
        "wins": row.wins,
        "losses": row.losses,
        "draws": row.draws,
        "win_rate": round(win_rate, 2),
        "total_rounds_won": row.total_rounds_won or 0,
        "total_rounds_lost": row.total_rounds_lost or 0,
        "avg_rounds_won": round(row.avg_rounds_won or 0, 2),
        "avg_rounds_lost": round(row.avg_rounds_lost or 0, 2),
    }

# ════════════════════════════════ TEAM ↔ TOURNAMENTS ════════════════════════════════


async def get_team_tournaments(db: AsyncSession, team_id: int):
    slug = (
        await db.execute(select(Team.slug).where(Team.id == team_id))
    ).scalar()
    if not slug:
        return []

    stmt = text(
        """
        SELECT DISTINCT
            t.id,
            t.name,
            t.logo,
            t.organizer,
            t.start_date,
            t.end_date,
            COUNT(DISTINCT m.id) AS matches_played,
            MIN(m.date)          AS first_match,
            MAX(m.date)          AS last_match,
            SUM(
                CASE
                    WHEN (m.team_match_info_a = tmi.id AND tmi.score > tmi_opponent.score) OR
                         (m.team_match_info_b = tmi.id AND tmi.score > tmi_opponent.score)
                    THEN 1 ELSE 0
                END
            )                    AS wins,
            COUNT(DISTINCT m.id) AS total_matches
        FROM tournaments       t
        JOIN matches           m   ON m.tournament_id = t.id
        JOIN team_match_info   tmi ON m.team_match_info_a = tmi.id
                                   OR m.team_match_info_b = tmi.id
        JOIN team_match_info   tmi_opponent
                                      ON (
                                          (m.team_match_info_a = tmi.id AND m.team_match_info_b = tmi_opponent.id) OR
                                          (m.team_match_info_b = tmi.id AND m.team_match_info_a = tmi_opponent.id)
                                      )
        WHERE tmi.team_slug = :team_slug
        GROUP BY t.id
        ORDER BY MAX(m.date) DESC
    """
    )
    result = await db.execute(stmt, {"team_slug": slug})
    return result.all()

# ════════════════════════════════ TEAM MAP STATS ════════════════════════════════


async def get_team_map_stats(db: AsyncSession, team_id: int):
    slug = (
        await db.execute(select(Team.slug).where(Team.id == team_id))
    ).scalar()
    if not slug:
        return {
            "team_id": team_id,
            "overall_stats": {
                "total_matches": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_draws": 0,
                "total_maps_played": 0,
                "overall_winrate": 0,
            },
            "maps": [],
        }

    stmt = text(
        """
        WITH team_matches AS (
            SELECT
                m.id           AS match_id,
                m.date,
                m.mapa         AS mapa,
                m.tournament_id,
                t.name         AS tournament_name,
                CASE WHEN tmi.id = m.team_match_info_a THEN tmi_a.score
                     ELSE                                         tmi_b.score END AS team_score,
                CASE WHEN tmi.id = m.team_match_info_a THEN tmi_b.score
                     ELSE                                         tmi_a.score END AS opponent_score,
                CASE WHEN tmi.id = m.team_match_info_a THEN t_b.id  ELSE t_a.id  END AS opponent_id,
                CASE WHEN tmi.id = m.team_match_info_a THEN t_b.name ELSE t_a.name END AS opponent_name,
                CASE WHEN tmi.id = m.team_match_info_a THEN tmi_a.score - tmi_b.score
                     ELSE                                         tmi_b.score - tmi_a.score END AS margin
            FROM matches m
            JOIN team_match_info tmi
                 ON (tmi.id = m.team_match_info_a OR tmi.id = m.team_match_info_b)
                AND tmi.team_slug = :team_slug
            JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
            JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
            JOIN teams t_a             ON tmi_a.team_slug = t_a.slug
            JOIN teams t_b             ON tmi_b.team_slug = t_b.slug
            LEFT JOIN tournaments t    ON m.tournament_id = t.id
        ),
        map_stats AS (
            SELECT
                mapa,
                COUNT(*)                                                        AS total_matches,
                SUM(CASE WHEN team_score > opponent_score THEN 1 END)           AS wins,
                SUM(CASE WHEN team_score < opponent_score THEN 1 END)           AS losses,
                SUM(CASE WHEN team_score = opponent_score THEN 1 END)           AS draws,
                SUM(team_score)                                                 AS total_rounds_won,
                SUM(opponent_score)                                             AS total_rounds_lost,
                AVG(team_score)                                                 AS avg_rounds_won,
                AVG(opponent_score)                                             AS avg_rounds_lost,
                MIN(date)                                                       AS first_played,
                MAX(date)                                                       AS last_played
            FROM team_matches
            GROUP BY mapa
        ),
        biggest_wins AS (
            SELECT DISTINCT ON (mapa)
                mapa,
                margin                         AS biggest_win_margin,
                match_id,
                date,
                opponent_id,
                opponent_name,
                team_score,
                opponent_score,
                tournament_id,
                tournament_name
            FROM team_matches
            WHERE margin > 0
            ORDER BY mapa, margin DESC, date DESC
        ),
        biggest_losses AS (
            SELECT DISTINCT ON (mapa)
                mapa,
                ABS(margin)                   AS biggest_loss_margin,
                match_id,
                date,
                opponent_id,
                opponent_name,
                team_score,
                opponent_score,
                tournament_id,
                tournament_name
            FROM team_matches
            WHERE margin < 0
            ORDER BY mapa, ABS(margin) DESC, date DESC
        ),
        total_stats AS (
            SELECT
                COUNT(DISTINCT mapa)                                         AS total_maps_played,
                COUNT(*)                                                     AS total_matches,
                SUM(CASE WHEN team_score > opponent_score THEN 1 END)        AS total_wins,
                SUM(CASE WHEN team_score < opponent_score THEN 1 END)        AS total_losses,
                SUM(CASE WHEN team_score = opponent_score THEN 1 END)        AS total_draws
            FROM team_matches
        )
        SELECT
            ms.*,
            ts.total_maps_played,
            ts.total_matches         AS overall_total_matches,
            ts.total_wins            AS overall_total_wins,
            ts.total_losses          AS overall_total_losses,
            ts.total_draws           AS overall_total_draws,
            bw.*,
            bl.*
        FROM map_stats ms
        CROSS JOIN total_stats ts
        LEFT JOIN biggest_wins   bw ON ms.mapa = bw.mapa
        LEFT JOIN biggest_losses bl ON ms.mapa = bl.mapa
        ORDER BY ms.total_matches DESC, ms.wins DESC
    """
    )

    rows = (await db.execute(stmt, {"team_slug": slug})).fetchall()
    if not rows:
        return {
            "team_id": team_id,
            "overall_stats": {
                "total_matches": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_draws": 0,
                "total_maps_played": 0,
                "overall_winrate": 0,
            },
            "maps": [],
        }

    overall = rows[0]
    overall_winrate = (
        overall.overall_total_wins / overall.overall_total_matches * 100
        if overall.overall_total_matches
        else 0
    )
    overall_stats = {
        "total_matches": overall.overall_total_matches,
        "total_wins": overall.overall_total_wins,
        "total_losses": overall.overall_total_losses,
        "total_draws": overall.overall_total_draws,
        "total_maps_played": overall.total_maps_played,
        "overall_winrate": round(overall_winrate, 2),
    }

    maps_stats = []
    for r in rows:
        total_matches = r.total_matches
        winrate = r.wins / total_matches * 100 if total_matches else 0
        playrate = (
            total_matches / overall.overall_total_matches * 100
            if overall.overall_total_matches
            else 0
        )
        total_rounds = r.total_rounds_won + r.total_rounds_lost

        def margin_detail(prefix: str):
            margin = getattr(r, f"{prefix}est_{'win' if prefix=='bigg' else 'loss'}_margin", None)
            if margin is None:
                return {"margin": 0, "match": None}
            match = {
                "date": getattr(r, f"{prefix}est_date").isoformat(),
                "opponent": getattr(r, f"{prefix}est_opponent_name"),
                "opponent_id": getattr(r, f"{prefix}est_opponent_id"),
                "score": f"{getattr(r, f'{prefix}est_team_score')}-{getattr(r, f'{prefix}est_opponent_score')}",
                "tournament": getattr(r, f"{prefix}est_tournament_name"),
                "tournament_id": getattr(r, f"{prefix}est_tournament_id"),
            }
            return {"margin": margin, "match": match}

        maps_stats.append(
            {
                "map_name": r.mapa,
                "matches_played": total_matches,
                "wins": r.wins,
                "losses": r.losses,
                "draws": r.draws,
                "playrate_percent": round(playrate, 2),
                "winrate_percent": round(winrate, 2),
                "rounds": {
                    "total_won": r.total_rounds_won,
                    "total_lost": r.total_rounds_lost,
                    "avg_won_per_match": round(r.avg_rounds_won or 0, 2),
                    "avg_lost_per_match": round(r.avg_rounds_lost or 0, 2),
                    "round_winrate_percent": round(
                        (r.total_rounds_won / total_rounds * 100)
                        if total_rounds
                        else 0,
                        2,
                    ),
                },
                "margins": {
                    "biggest_win": margin_detail("biggest_win"),
                    "biggest_loss": margin_detail("biggest_loss"),
                },
                "dates": {
                    "first_played": r.first_played.isoformat()
                    if r.first_played
                    else None,
                    "last_played": r.last_played.isoformat()
                    if r.last_played
                    else None,
                },
            }
        )

    return {"team_id": team_id, "overall_stats": overall_stats, "maps": maps_stats}

# ════════════════════════════════ MATCHES ════════════════════════════════


async def get_team_matches(
    db: AsyncSession, team_id: int, limit: int = 50
) -> List[Match]:
    slug_subq = select(Team.slug).where(Team.id == team_id).scalar_subquery()

    stmt = (
        select(Match)
        .join(
            TeamMatchInfo,
            (Match.team_match_info_a == TeamMatchInfo.id)
            | (Match.team_match_info_b == TeamMatchInfo.id),
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
    result = await db.execute(stmt)
    return result.scalars().all()


async def list_matches(db: AsyncSession, limit: int = 20) -> List[Match]:
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
    logger.info(f"Matches encontrados: {len(matches)}")
    return matches


async def get_match(db: AsyncSession, match_id: uuid.UUID) -> Optional[Match]:
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
    return result.scalar_one_or_none()

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════


async def list_tournaments(db: AsyncSession) -> List[Tournament]:
    result = await db.execute(
        select(Tournament).order_by(Tournament.start_date.desc())
    )
    return result.scalars().all()


async def get_tournament(
    db: AsyncSession, tournament_id: uuid.UUID
) -> Optional[Tournament]:
    result = await db.execute(
        select(Tournament).where(Tournament.id == tournament_id)
    )
    return result.scalar_one_or_none()


async def get_tournament_matches(
    db: AsyncSession, tournament_id: uuid.UUID
) -> List[Match]:
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
    return result.scalars().all()

# ════════════════════════════════ MAPS PLAYED ════════════════════════════════


async def get_maps_played(db: AsyncSession) -> Dict[str, Any]:
    stmt = text(
        """
        SELECT
            mapa AS map,
            COUNT(*)                     AS times_played,
            COUNT(DISTINCT DATE(date))   AS days_played
        FROM matches
        WHERE mapa IS NOT NULL
        GROUP BY mapa
        ORDER BY times_played DESC
    """
    )
    result = await db.execute(stmt)
    maps = {row.map: {"times_played": row.times_played, "days_played": row.days_played} for row in result}
    return {"maps": maps, "total_maps": len(maps)}

# ════════════════════════════════ ESTADOS / RANKING ════════════════════════════════


async def get_estados_ranking_summary(db: AsyncSession) -> Dict[str, Any]:
    if not RANKING_AVAILABLE:
        return {"error": "Sistema de ranking não disponível"}

    ranking_data = await calculate_ranking(db, include_variation=False)

    estado_stats: Dict[str, Any] = {}
    for team_data in ranking_data:
        team_id = team_data.get("team_id")
        if not team_id:
            continue

        q = (
            select(Team.estado_id, Estado.sigla, Estado.nome)
            .join(Estado, Team.estado_id == Estado.id)
            .where(Team.id == team_id)
        )
        r = (await db.execute(q)).first()
        if not r or not r.estado_id:
            continue

        sigla = r.sigla
        estado_stats.setdefault(
            sigla,
            {
                "nome": r.nome,
                "teams": [],
                "count": 0,
                "avg_nota": 0.0,
                "max_nota": 0.0,
                "min_nota": 100.0,
                "total_games": 0,
            },
        )
        est = estado_stats[sigla]

        nota = team_data["nota_final"]
        est["teams"].append(
            {
                "name": team_data["team"],
                "position": team_data["posicao"],
                "nota": nota,
            }
        )
        est["count"] += 1
        est["total_games"] += team_data["games_count"]
        est["max_nota"] = max(est["max_nota"], nota)
        est["min_nota"] = min(est["min_nota"], nota)

    ranking = []
    for sigla, st in estado_stats.items():
        if st["count"] == 0:
            continue
        st["avg_nota"] = st["avg_nota"] = sum(t["nota"] for t in st["teams"]) / st["count"]
        st["avg_games_per_team"] = st["total_games"] / st["count"]
        ranking.append(
            {
                "sigla": sigla,
                "nome": st["nome"],
                "teams_count": st["count"],
                "avg_nota": round(st["avg_nota"], 2),
                "max_nota": round(st["max_nota"], 2),
                "min_nota": round(st["min_nota"], 2),
                "avg_games_per_team": round(st["avg_games_per_team"], 1),
                "top_team": max(st["teams"], key=lambda x: x["nota"]),
            }
        )

    ranking.sort(key=lambda x: x["avg_nota"], reverse=True)
    return {"total_estados": len(ranking), "ranking": ranking}