# crud.py
import uuid
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from models import (
    Team,
    TeamPlayer,
    Tournament,
    TeamMatchInfo,
    Match,
)


# ───────────────────────────  TEAMS  ────────────────────────────
async def list_teams(db: AsyncSession) -> list[Team]:
    """
    Retorna todos os times ordenados alfabeticamente.
    """
    res = await db.execute(
        select(Team).order_by(Team.name.asc())
    )
    return res.scalars().all()


async def get_team(db: AsyncSession, team_id: int) -> Team | None:
    """
    Busca um time pelo ID
    """
    res = await db.execute(
        select(Team).where(Team.id == team_id)
    )
    return res.scalar_one_or_none()


async def get_team_matches(
    db: AsyncSession, 
    team_id: int,
    limit: int = 50
) -> list[Match]:
    """
    Retorna todas as partidas de um time específico
    """
    # Subconsulta para pegar os team_match_info do time
    tmi_subquery = select(TeamMatchInfo.id).where(
        TeamMatchInfo.team_id == team_id
    ).subquery()
    
    # Busca partidas onde o time está em team_a ou team_b
    stmt = (
        select(Match)
        .where(
            sa.or_(
                Match.team_match_info_a.in_(select(tmi_subquery)),
                Match.team_match_info_b.in_(select(tmi_subquery))
            )
        )
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date.desc())
        .limit(limit)
    )
    
    res = await db.execute(stmt)
    return res.scalars().unique().all()

async def get_team_players(
    db: AsyncSession,
    team_id: int
) -> list[str]:
    """Retorna os nomes dos jogadores de um time"""
    stmt = select(TeamPlayer.player_nick).where(
        TeamPlayer.team_id == team_id
    ).order_by(TeamPlayer.player_nick)
    
    result = await db.execute(stmt)
    return [nick for (nick,) in result]


# ─────────────────────────  TOURNAMENTS  ─────────────────────────
async def list_tournaments(db: AsyncSession) -> list[Tournament]:
    """
    Lista torneios ordenados pela data de início (decrescente).
    Caso 'startsOn' seja NULL, usa 'created_at' como fallback.
    """
    res = await db.execute(
        select(Tournament).order_by(
            sa.desc(Tournament.startsOn), 
            sa.desc(Tournament.created_at)
        )
    )
    return res.scalars().all()


async def get_tournament(
    db: AsyncSession, 
    tournament_id: uuid.UUID
) -> Tournament | None:
    """
    Busca um torneio pelo ID
    """
    res = await db.execute(
        select(Tournament).where(Tournament.id == tournament_id)
    )
    return res.scalar_one_or_none()


async def get_tournament_matches(
    db: AsyncSession,
    tournament_id: uuid.UUID
) -> list[Match]:
    """
    Retorna todas as partidas de um torneio
    """
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
    
    res = await db.execute(stmt)
    return res.scalars().unique().all()


# ───────────────────────────  MATCHES  ───────────────────────────
async def list_matches(
    db: AsyncSession,
    limit: int = 20,
    offset: int = 0,
    tournament_id: uuid.UUID | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> list[Match]:
    """
    Retorna as partidas com filtros opcionais
    """
    stmt = select(Match)
    
    # Filtros opcionais
    if tournament_id:
        stmt = stmt.where(Match.tournament_id == tournament_id)
    
    if start_date:
        stmt = stmt.where(Match.date >= start_date)
    
    if end_date:
        stmt = stmt.where(Match.date <= end_date)
    
    # Adiciona joins e ordenação
    stmt = (
        stmt.options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
        .order_by(Match.date.desc())
        .offset(offset)
        .limit(limit)
    )
    
    res = await db.execute(stmt)
    return res.scalars().unique().all()


async def get_match(
    db: AsyncSession,
    match_id: uuid.UUID,
) -> Match | None:
    """
    Busca uma partida pelo UUID, trazendo dados relacionados.
    """
    stmt = (
        select(Match)
        .where(Match.id == match_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
            selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
        )
    )
    res = await db.execute(stmt)
    return res.scalar_one_or_none()


# ───────────────────────────  STATS  ───────────────────────────
async def get_team_stats(db: AsyncSession, team_id: int) -> dict:
    """
    Retorna estatísticas de um time
    """
    # Total de partidas
    tmi_subquery = select(TeamMatchInfo.id).where(
        TeamMatchInfo.team_id == team_id
    ).subquery()
    
    total_matches = await db.execute(
        select(func.count(Match.id)).where(
            sa.or_(
                Match.team_match_info_a.in_(select(tmi_subquery)),
                Match.team_match_info_b.in_(select(tmi_subquery))
            )
        )
    )
    total = total_matches.scalar() or 0
    
    # Vitórias (quando score do time > score do oponente)
    wins_stmt = """
    SELECT COUNT(*) FROM matches m
    JOIN team_match_info tmi_team ON (m.team_match_info_a = tmi_team.id OR m.team_match_info_b = tmi_team.id)
    JOIN team_match_info tmi_opponent ON (
        (m.team_match_info_a = tmi_opponent.id AND m.team_match_info_b = tmi_team.id) OR
        (m.team_match_info_b = tmi_opponent.id AND m.team_match_info_a = tmi_team.id)
    )
    WHERE tmi_team.team_id = :team_id 
    AND tmi_team.score > tmi_opponent.score
    """
    
    wins_result = await db.execute(sa.text(wins_stmt), {"team_id": team_id})
    wins = wins_result.scalar() or 0
    
    return {
        "team_id": team_id,
        "total_matches": total,
        "wins": wins,
        "losses": total - wins,
        "win_rate": round((wins / total * 100), 2) if total > 0 else 0
    }


async def get_maps_played(db: AsyncSession) -> list[dict]:
    """
    Retorna a contagem de partidas por mapa
    """
    stmt = (
        select(
            Match.map,
            func.count(Match.id).label("count")
        )
        .group_by(Match.map)
        .order_by(func.count(Match.id).desc())
    )
    
    res = await db.execute(stmt)
    return [
        {"map": row.map, "count": row.count}
        for row in res
    ]

async def search_teams(
    db: AsyncSession, 
    query: str = None,
    university: str = None,
    limit: int = 20
) -> list[Team]:
    """
    Busca times por nome, slug ou universidade
    """
    stmt = select(Team)
    
    if query:
        # Busca em nome, slug e tag
        search = f"%{query}%"
        stmt = stmt.where(
            sa.or_(
                Team.name.ilike(search),
                Team.slug.ilike(search),
                Team.tag.ilike(search)
            )
        )
    
    if university:
        stmt = stmt.where(Team.university.ilike(f"%{university}%"))
    
    stmt = stmt.order_by(Team.name).limit(limit)
    
    res = await db.execute(stmt)
    return res.scalars().all()