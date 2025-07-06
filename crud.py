# crud.py
import uuid
from datetime import datetime
from venv import logger
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

def clean_agent_value(agent: str | None) -> str | None:
    """Limpa valores inválidos de agentes"""
    if agent == '?' or agent == '':
        return None
    return agent

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
    Retorna todas as partidas de um time específico com tratamento de erro aprimorado
    """
    try:
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
        matches = res.scalars().unique().all()
        
        # Valida cada partida antes de retornar
        validated_matches = []
        for match in matches:
            try:
                # Verifica se os dados essenciais existem
                if not match.tmi_a or not match.tmi_b:
                    logger.warning(f"Partida {match.id} sem team_match_info completo")
                    continue
                
                # Limpa agentes inválidos
                if hasattr(match.tmi_a, 'agent_1'):
                    for i in range(1, 6):
                        agent_attr = f'agent_{i}'
                        agent_value = getattr(match.tmi_a, agent_attr, None)
                        if agent_value == '?':
                            setattr(match.tmi_a, agent_attr, None)
                        
                if hasattr(match.tmi_b, 'agent_1'):
                    for i in range(1, 6):
                        agent_attr = f'agent_{i}'
                        agent_value = getattr(match.tmi_b, agent_attr, None)
                        if agent_value == '?':
                            setattr(match.tmi_b, agent_attr, None)
                
                validated_matches.append(match)
                
            except Exception as e:
                logger.error(f"Erro ao validar partida {match.id}: {str(e)}")
                continue
        
        return validated_matches
        
    except Exception as e:
        logger.error(f"Erro em get_team_matches para time {team_id}: {str(e)}")
        raise