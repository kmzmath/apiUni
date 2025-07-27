# crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func, and_
from sqlalchemy.orm import selectinload, joinedload
from typing import List, Optional, Dict, Any
import uuid
import logging
from datetime import datetime, timezone

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer, Estado, Agent, Map
import schemas

# Configurar logging
logger = logging.getLogger(__name__)

# Importações condicionais para o sistema de ranking
try:
    from ranking import calculate_ranking
    RANKING_AVAILABLE = True
except ImportError:
    logger.warning("Sistema de ranking não disponível")
    RANKING_AVAILABLE = False
    async def calculate_ranking(db, include_variation=True):
        return []

# ════════════════════════════════ TEAMS ════════════════════════════════

async def get_team(db: AsyncSession, team_id: int) -> Optional[schemas.Team]:
    """Busca um time pelo ID - SEMPRE com estado"""
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
    team = result.scalar_one_or_none()
    return team

async def list_teams(db: AsyncSession) -> List[schemas.Team]:
    """Lista todos os times - SEMPRE com estado"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .order_by(Team.name)
    )
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

async def list_teams_minimal(db: AsyncSession) -> List[dict]:
    """Lista minimal de times com apenas sigla e ícone do estado"""
    stmt = text("""
        SELECT 
            t.id,
            t.name,
            t.tag,
            t.logo,
            e.sigla as estado_sigla,
            e.icone as estado_icone
        FROM teams t
        LEFT JOIN estados e ON t.estado_id = e.id
        ORDER BY t.name
    """)
    
    result = await db.execute(stmt)
    teams = []
    for row in result:
        teams.append({
            "id": row.id,
            "name": row.name,
            "tag": row.tag,
            "logo": row.logo,
            "estado_sigla": row.estado_sigla,
            "estado_icone": row.estado_icone
        })
    return teams

async def search_teams(
    db: AsyncSession, 
    query: Optional[str] = None,
    university: Optional[str] = None,
    limit: int = 20
) -> List[schemas.Team]:
    """Busca times com filtros - SEMPRE com estado"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
    )
    
    if query:
        search_term = f"%{query}%"
        stmt = stmt.where(
            (Team.name.ilike(search_term)) |
            (Team.tag.ilike(search_term)) |
            (Team.slug.ilike(search_term))
        )
    
    if university:
        stmt = stmt.where(Team.org.ilike(f"%{university}%"))
    
    stmt = stmt.order_by(Team.name).limit(limit)
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

async def get_team_stats(db: AsyncSession, team_id: int) -> dict[str, any]:
    """Retorna estatísticas de vitórias/derrotas de um time"""
    try:
        # Primeiro, busca o slug do time
        team = await get_team(db, team_id)
        if not team:
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
        
        # Query adaptada para a estrutura atual
        stmt = text("""
            WITH team_matches AS (
                SELECT 
                    m."idPartida",
                    CASE 
                        WHEN m.team_i = :team_slug THEN m.score_i
                        ELSE m.score_j
                    END as team_score,
                    CASE 
                        WHEN m.team_i = :team_slug THEN m.score_j
                        ELSE m.score_i
                    END as opponent_score
                FROM matches m
                WHERE m.team_i = :team_slug OR m.team_j = :team_slug
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
        
        result = await db.execute(stmt, {"team_slug": team.slug})
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
        # Busca o slug do time
        team = await get_team(db, team_id)
        if not team:
            return []
        
        stmt = text("""
            SELECT DISTINCT
                t.id,
                t.name,
                t.logo,
                t.organizer,
                t.start_date as starts_on,
                t.end_date as ends_on,
                COUNT(DISTINCT m."idPartida") as matches_played,
                MIN(m.date) as first_match,
                MAX(m.date) as last_match,
                SUM(CASE 
                    WHEN (m.team_i = :team_slug AND m.score_i > m.score_j) OR
                         (m.team_j = :team_slug AND m.score_j > m.score_i)
                    THEN 1 ELSE 0 
                END) as wins,
                COUNT(DISTINCT m."idPartida") as total_matches
            FROM tournaments t
            JOIN matches m ON m.campeonato = t.name
            WHERE m.team_i = :team_slug OR m.team_j = :team_slug
            GROUP BY t.id, t.name, t.logo, t.organizer, t.start_date, t.end_date
            ORDER BY MAX(m.date) DESC
        """)
        
        result = await db.execute(stmt, {"team_slug": team.slug})
        return result.all()
    except Exception as e:
        logger.error(f"Erro ao buscar torneios do time {team_id}: {e}")
        return []

async def get_team_map_stats(db: AsyncSession, team_id: int):
    """Retorna estatísticas detalhadas de mapas para um time específico"""
    
    # Busca o slug do time
    team = await get_team(db, team_id)
    if not team:
        return {
            "team_id": team_id,
            "overall_stats": {
                "total_matches": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_draws": 0,
                "total_maps_played": 0,
                "overall_winrate": 0
            },
            "maps": []
        }
    
    # Query adaptada para a estrutura atual
    stmt = text("""
        WITH team_matches AS (
            SELECT 
                m."idPartida" as match_id,
                m.date,
                m.mapa as map,
                m.campeonato as tournament_name,
                CASE 
                    WHEN m.team_i = :team_slug THEN m.score_i
                    ELSE m.score_j
                END as team_score,
                CASE 
                    WHEN m.team_i = :team_slug THEN m.score_j
                    ELSE m.score_i
                END as opponent_score,
                CASE 
                    WHEN m.team_i = :team_slug THEN m.team_j
                    ELSE m.team_i
                END as opponent_slug,
                CASE 
                    WHEN m.team_i = :team_slug THEN m.score_i - m.score_j
                    ELSE m.score_j - m.score_i
                END as margin
            FROM matches m
            WHERE (m.team_i = :team_slug OR m.team_j = :team_slug)
              AND m.mapa IS NOT NULL
        ),
        map_stats AS (
            SELECT 
                map,
                COUNT(*) as total_matches,
                SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN team_score = opponent_score THEN 1 ELSE 0 END) as draws,
                SUM(team_score) as total_rounds_won,
                SUM(opponent_score) as total_rounds_lost,
                AVG(team_score) as avg_rounds_won,
                AVG(opponent_score) as avg_rounds_lost,
                MIN(date) as first_played,
                MAX(date) as last_played
            FROM team_matches
            GROUP BY map
        ),
        total_stats AS (
            SELECT 
                COUNT(DISTINCT map) as total_maps_played,
                COUNT(*) as total_matches,
                SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as total_wins,
                SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as total_losses,
                SUM(CASE WHEN team_score = opponent_score THEN 1 ELSE 0 END) as total_draws
            FROM team_matches
        )
        SELECT 
            ms.*,
            ts.total_maps_played,
            ts.total_matches as overall_total_matches,
            ts.total_wins as overall_total_wins,
            ts.total_losses as overall_total_losses,
            ts.total_draws as overall_total_draws
        FROM map_stats ms
        CROSS JOIN total_stats ts
        ORDER BY ms.total_matches DESC, ms.wins DESC
    """)
    
    result = await db.execute(stmt, {"team_slug": team.slug})
    rows = result.fetchall()
    
    if not rows:
        return {
            "team_id": team_id,
            "overall_stats": {
                "total_matches": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_draws": 0,
                "total_maps_played": 0,
                "overall_winrate": 0
            },
            "maps": []
        }
    
    # Processa os resultados
    maps_stats = []
    overall_stats = None
    
    for row in rows:
        if overall_stats is None:
            overall_winrate = (row.overall_total_wins / row.overall_total_matches * 100) if row.overall_total_matches > 0 else 0
            overall_stats = {
                "total_matches": row.overall_total_matches,
                "total_wins": row.overall_total_wins,
                "total_losses": row.overall_total_losses,
                "total_draws": row.overall_total_draws,
                "total_maps_played": row.total_maps_played,
                "overall_winrate": round(overall_winrate, 2)
            }
        
        # Calcula estatísticas do mapa
        total_matches = row.total_matches
        winrate = (row.wins / total_matches * 100) if total_matches > 0 else 0
        playrate = (total_matches / row.overall_total_matches * 100) if row.overall_total_matches > 0 else 0
        total_rounds = row.total_rounds_won + row.total_rounds_lost
        
        map_stat = {
            "map_name": row.map,
            "matches_played": total_matches,
            "wins": row.wins,
            "losses": row.losses,
            "draws": row.draws,
            "playrate_percent": round(playrate, 2),
            "winrate_percent": round(winrate, 2),
            "rounds": {
                "total_won": row.total_rounds_won,
                "total_lost": row.total_rounds_lost,
                "avg_won_per_match": round(row.avg_rounds_won, 2),
                "avg_lost_per_match": round(row.avg_rounds_lost, 2),
                "round_winrate_percent": round((row.total_rounds_won / total_rounds * 100) if total_rounds > 0 else 0, 2)
            },
            "margins": {
                "biggest_win": {"margin": 0, "match": None},
                "biggest_loss": {"margin": 0, "match": None}
            },
            "dates": {
                "first_played": row.first_played.isoformat() if row.first_played else None,
                "last_played": row.last_played.isoformat() if row.last_played else None
            }
        }
        
        maps_stats.append(map_stat)
    
    return {
        "team_id": team_id,
        "overall_stats": overall_stats,
        "maps": maps_stats
    }

# ════════════════════════════════ MATCHES ════════════════════════════════


async def list_matches(db: AsyncSession, limit: int = 20) -> List[schemas.Match]:
    """Lista as partidas mais recentes"""
    try:
        stmt = (
            select(Match)
            .options(
                selectinload(Match.tournament),
                selectinload(Match.team_i_obj),
                selectinload(Match.team_j_obj),
            )
            .order_by(Match.date.desc())
            .limit(limit)
        )
        
        result = await db.execute(stmt)
        matches = result.scalars().all()
        
        # Formata as partidas para compatibilidade
        formatted_matches = []
        for match in matches:
            # Cria objetos TMI compatíveis
            tmi_a = TeamMatchInfo()
            tmi_a.id = match.tmi_a or uuid.uuid4()
            tmi_a.team = match.team_i_obj
            tmi_a.score = match.score_i
            
            tmi_b = TeamMatchInfo()
            tmi_b.id = match.tmi_b or uuid.uuid4()
            tmi_b.team = match.team_j_obj
            tmi_b.score = match.score_j
            
            match.tmi_a = tmi_a
            match.tmi_b = tmi_b
            formatted_matches.append(match)
        
        return formatted_matches
    except Exception as e:
        logger.error(f"Erro em list_matches: {str(e)}", exc_info=True)
        raise

async def get_match(db: AsyncSession, match_id: str) -> Optional[schemas.Match]:
    """Busca uma partida específica"""
    stmt = (
        select(Match)
        .where(Match.idPartida == match_id)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.team_i_obj),
            selectinload(Match.team_j_obj),
        )
    )
    
    result = await db.execute(stmt)
    match = result.scalar_one_or_none()
    
    if match:
        # Formata para compatibilidade
        tmi_a = TeamMatchInfo()
        tmi_a.id = match.tmi_a or uuid.uuid4()
        tmi_a.team = match.team_i_obj
        tmi_a.score = match.score_i
        
        tmi_b = TeamMatchInfo()
        tmi_b.id = match.tmi_b or uuid.uuid4()
        tmi_b.team = match.team_j_obj
        tmi_b.score = match.score_j
        
        match.tmi_a = tmi_a
        match.tmi_b = tmi_b
    
    return match


async def get_team_matches(db: AsyncSession, team_id: int, limit: int = 50) -> List[schemas.Match]:
    """Busca todas as partidas de um time"""
    try:
        # Busca o time primeiro para pegar o slug
        team = await get_team(db, team_id)
        if not team:
            return []
        
        # Query para buscar partidas onde o time participa
        stmt = text("""
            SELECT DISTINCT
                m.id,
                m.tournament_name,
                m.tournament_icon,
                m.date,
                m.stage,
                m.playoffs,
                m.mapa AS map,
                -- Time A
                tmi_a.team_name as team_a_name,
                tmi_a.team_tag as team_a_tag,
                tmi_a.team_slug as team_a_slug,
                tmi_a.team_image as team_a_logo,
                tmi_a.score as team_a_score,
                -- Time B
                tmi_b.team_name as team_b_name,
                tmi_b.team_tag as team_b_tag,
                tmi_b.team_slug as team_b_slug,
                tmi_b.team_image as team_b_logo,
                tmi_b.score as team_b_score
            FROM matches m
            JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
            JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
            WHERE tmi_a.team_slug = :team_slug OR tmi_b.team_slug = :team_slug
            ORDER BY m.date DESC
            LIMIT :limit
        """)
        
        result = await db.execute(stmt, {
            "team_slug": team.slug,
            "limit": limit
        })
        
        matches = []
        for row in result:
            match_data = {
                "id": str(row.id),
                "tournament_name": row.tournament_name,
                "tournament_icon": row.tournament_icon,
                "date": row.date.isoformat() if row.date else None,
                "stage": row.stage,
                "playoffs": row.playoffs,
                "map": row.map,
                "team_a": {
                    "name": row.team_a_name,
                    "tag": row.team_a_tag,
                    "slug": row.team_a_slug,
                    "logo": row.team_a_logo,
                    "score": row.team_a_score
                },
                "team_b": {
                    "name": row.team_b_name,
                    "tag": row.team_b_tag,
                    "slug": row.team_b_slug,
                    "logo": row.team_b_logo,
                    "score": row.team_b_score
                }
            }
            matches.append(match_data)
        
        return matches
        
    except Exception as e:
        logger.error(f"Erro em get_team_matches: {str(e)}")
        return []

# ════════════════════════════════ TOURNAMENTS ════════════════════════════════

async def list_tournaments(db: AsyncSession) -> List[schemas.Tournament]:
    """Lista todos os torneios"""
    result = await db.execute(
        select(Tournament).order_by(Tournament.start_date.desc().nullslast())
    )
    tournaments = result.scalars().all()
    return tournaments

async def get_tournament(
    db: AsyncSession, 
    tournament_id: int
) -> Optional[schemas.Tournament]:
    """Busca um torneio específico"""
    result = await db.execute(
        select(Tournament).where(Tournament.id == tournament_id)
    )
    tournament = result.scalar_one_or_none()
    return tournament

async def get_tournament_matches(
    db: AsyncSession, 
    tournament_id: int
) -> List[schemas.Match]:
    """Retorna todas as partidas de um torneio"""
    # Busca o nome do torneio
    tournament = await get_tournament(db, tournament_id)
    if not tournament:
        return []
    
    stmt = (
        select(Match)
        .where(Match.campeonato == tournament.name)
        .options(
            selectinload(Match.tournament),
            selectinload(Match.team_i_obj),
            selectinload(Match.team_j_obj),
        )
        .order_by(Match.date.desc())
    )
    
    result = await db.execute(stmt)
    matches = result.scalars().all()
    
    # Formata as partidas
    formatted_matches = []
    for match in matches:
        tmi_a = TeamMatchInfo()
        tmi_a.id = match.tmi_a or uuid.uuid4()
        tmi_a.team = match.team_i_obj
        tmi_a.score = match.score_i
        
        tmi_b = TeamMatchInfo()
        tmi_b.id = match.tmi_b or uuid.uuid4()
        tmi_b.team = match.team_j_obj
        tmi_b.score = match.score_j
        
        match.tmi_a = tmi_a
        match.tmi_b = tmi_b
        formatted_matches.append(match)
    
    return formatted_matches

# ════════════════════════════════ STATS ════════════════════════════════

async def get_maps_played(db: AsyncSession) -> dict:
    """Retorna estatísticas de mapas jogados"""
    stmt = text("""
        SELECT 
            mapa as map,
            COUNT(*) as times_played,
            COUNT(DISTINCT DATE(date)) as days_played
        FROM matches
        WHERE mapa IS NOT NULL
        GROUP BY mapa
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

# ════════════════════════════════ ESTADOS ════════════════════════════════

async def search_teams_by_estado(
    db: AsyncSession,
    estado_id: Optional[int] = None,
    sigla: Optional[str] = None,
    regiao: Optional[str] = None,
    limit: int = 50
) -> List[Team]:
    """Busca times filtrados por estado/região"""
    stmt = select(Team).options(selectinload(Team.estado_obj))
    
    if estado_id:
        stmt = stmt.where(Team.estado_id == estado_id)
    elif sigla:
        stmt = stmt.join(Estado).where(Estado.sigla == sigla.upper())
    elif regiao:
        stmt = stmt.join(Estado).where(Estado.regiao == regiao)
    
    stmt = stmt.order_by(Team.name).limit(limit)
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

async def get_estados_ranking_summary(db: AsyncSession) -> dict[str, Any]:
    """Retorna um resumo do ranking por estado"""
    if not RANKING_AVAILABLE:
        return {"error": "Sistema de ranking não disponível"}
    
    try:
        # Calcula o ranking atual
        ranking_data = await calculate_ranking(db, include_variation=False)
        
        # Agrupa por estado
        estado_stats = {}
        
        for team_data in ranking_data:
            team_id = team_data.get("team_id")
            if not team_id:
                continue
            
            # Busca o estado do time
            team_stmt = select(Team.estado_id, Estado.sigla, Estado.nome).join(Estado).where(Team.id == team_id)
            result = await db.execute(team_stmt)
            team_info = result.first()
            
            if team_info and team_info.estado_id:
                sigla = team_info.sigla
                if sigla not in estado_stats:
                    estado_stats[sigla] = {
                        "nome": team_info.nome,
                        "teams": [],
                        "count": 0,
                        "avg_nota": 0,
                        "max_nota": 0,
                        "min_nota": 100,
                        "total_games": 0
                    }
                
                nota = team_data["nota_final"]
                estado_stats[sigla]["teams"].append({
                    "name": team_data["team"],
                    "position": team_data["posicao"],
                    "nota": nota
                })
                estado_stats[sigla]["count"] += 1
                estado_stats[sigla]["total_games"] += team_data["games_count"]
                estado_stats[sigla]["max_nota"] = max(estado_stats[sigla]["max_nota"], nota)
                estado_stats[sigla]["min_nota"] = min(estado_stats[sigla]["min_nota"], nota)
        
        # Calcula médias e ordena
        estado_ranking = []
        for sigla, stats in estado_stats.items():
            if stats["count"] > 0:
                stats["avg_nota"] = sum(t["nota"] for t in stats["teams"]) / stats["count"]
                stats["avg_games_per_team"] = stats["total_games"] / stats["count"]
                
                # Remove a lista completa de times para o resumo
                summary_stats = {
                    "sigla": sigla,
                    "nome": stats["nome"],
                    "teams_count": stats["count"],
                    "avg_nota": round(stats["avg_nota"], 2),
                    "max_nota": round(stats["max_nota"], 2),
                    "min_nota": round(stats["min_nota"], 2),
                    "avg_games_per_team": round(stats["avg_games_per_team"], 1),
                    "top_team": max(stats["teams"], key=lambda x: x["nota"])
                }
                estado_ranking.append(summary_stats)
        
        # Ordena por nota média
        estado_ranking.sort(key=lambda x: x["avg_nota"], reverse=True)
        
        return {
            "total_estados": len(estado_ranking),
            "ranking": estado_ranking
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular ranking por estado: {e}")
        return {"error": str(e)}