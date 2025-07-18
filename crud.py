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
        .options(selectinload(Team.estado_obj))  # SEMPRE faz o join
        .where(Team.id == team_id)
    )
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    return team

async def list_teams(db: AsyncSession) -> List[schemas.Team]:
    """Lista todos os times - SEMPRE com estado"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))  # SEMPRE faz o join
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
        .options(selectinload(Team.estado_obj))  # SEMPRE faz o join
    )
    
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
                    m.id,
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN tmi_a.score
                        ELSE tmi_b.score
                    END as team_score,
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN tmi_b.score
                        ELSE tmi_a.score
                    END as opponent_score
                FROM matches m
                JOIN team_match_info tmi ON (
                    (tmi.id = m.team_match_info_a OR tmi.id = m.team_match_info_b) 
                    AND tmi.team_id = :team_id
                )
                JOIN team_match_info tmi_a ON m.team_match_info_a = tmi_a.id
                JOIN team_match_info tmi_b ON m.team_match_info_b = tmi_b.id
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

async def get_team_map_stats(db: AsyncSession, team_id: int):
    """
    Retorna estatísticas detalhadas de mapas para um time específico,
    incluindo detalhes das partidas com maiores margens.
    """
    
    # Query principal com CTEs para buscar estatísticas e detalhes das margens
    stmt = text("""
        WITH team_matches AS (
            -- Busca todas as partidas do time com detalhes
            SELECT 
                m.id as match_id,
                m.date,
                m.map,
                m.tournament_id,
                t.name as tournament_name,
                CASE 
                    WHEN tmi.id = m.team_match_info_a THEN tmi_a.score
                    ELSE tmi_b.score
                END as team_score,
                CASE 
                    WHEN tmi.id = m.team_match_info_a THEN tmi_b.score
                    ELSE tmi_a.score
                END as opponent_score,
                CASE 
                    WHEN tmi.id = m.team_match_info_a THEN t_b.id
                    ELSE t_a.id
                END as opponent_id,
                CASE 
                    WHEN tmi.id = m.team_match_info_a THEN t_b.name
                    ELSE t_a.name
                END as opponent_name,
                -- Calcula a margem (positiva para vitória, negativa para derrota)
                CASE 
                    WHEN tmi.id = m.team_match_info_a THEN tmi_a.score - tmi_b.score
                    ELSE tmi_b.score - tmi_a.score
                END as margin
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
        ),
        map_stats AS (
            -- Estatísticas gerais por mapa
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
        biggest_wins AS (
            -- Encontra a partida com maior margem de vitória por mapa
            SELECT DISTINCT ON (map)
                map,
                margin as biggest_win_margin,
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
            ORDER BY map, margin DESC, date DESC
        ),
        biggest_losses AS (
            -- Encontra a partida com maior margem de derrota por mapa
            SELECT DISTINCT ON (map)
                map,
                ABS(margin) as biggest_loss_margin,
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
            ORDER BY map, ABS(margin) DESC, date DESC
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
            ts.total_draws as overall_total_draws,
            bw.biggest_win_margin,
            bw.match_id as win_match_id,
            bw.date as win_date,
            bw.opponent_id as win_opponent_id,
            bw.opponent_name as win_opponent_name,
            bw.team_score as win_team_score,
            bw.opponent_score as win_opponent_score,
            bw.tournament_id as win_tournament_id,
            bw.tournament_name as win_tournament_name,
            bl.biggest_loss_margin,
            bl.match_id as loss_match_id,
            bl.date as loss_date,
            bl.opponent_id as loss_opponent_id,
            bl.opponent_name as loss_opponent_name,
            bl.team_score as loss_team_score,
            bl.opponent_score as loss_opponent_score,
            bl.tournament_id as loss_tournament_id,
            bl.tournament_name as loss_tournament_name
        FROM map_stats ms
        CROSS JOIN total_stats ts
        LEFT JOIN biggest_wins bw ON ms.map = bw.map
        LEFT JOIN biggest_losses bl ON ms.map = bl.map
        ORDER BY ms.total_matches DESC, ms.wins DESC
    """)
    
    result = await db.execute(stmt, {"team_id": team_id})
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
        
        # Monta os detalhes das margens
        biggest_win_detail = {
            "margin": row.biggest_win_margin or 0,
            "match": None
        }
        
        if row.win_match_id:
            biggest_win_detail["match"] = {
                "date": row.win_date.isoformat(),
                "opponent": row.win_opponent_name,
                "opponent_id": row.win_opponent_id,
                "score": f"{row.win_team_score}-{row.win_opponent_score}",
                "tournament": row.win_tournament_name,
                "tournament_id": row.win_tournament_id
            }
        
        biggest_loss_detail = {
            "margin": row.biggest_loss_margin or 0,
            "match": None
        }
        
        if row.loss_match_id:
            biggest_loss_detail["match"] = {
                "date": row.loss_date.isoformat(),
                "opponent": row.loss_opponent_name,
                "opponent_id": row.loss_opponent_id,
                "score": f"{row.loss_team_score}-{row.loss_opponent_score}",
                "tournament": row.loss_tournament_name,
                "tournament_id": row.loss_tournament_id
            }
        
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
                "biggest_win": biggest_win_detail,
                "biggest_loss": biggest_loss_detail
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
        
        # Log para debug
        logger.info(f"Matches encontradas: {len(matches)}")
        if matches and matches[0].tournament:
            logger.info(f"Tournament data: starts_on={matches[0].tournament.starts_on}, ends_on={matches[0].tournament.ends_on}")
        
        return matches
    except Exception as e:
        logger.error(f"Erro em list_matches: {str(e)}", exc_info=True)
        raise

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

# Adicionar/modificar no crud.py

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer, Estado

async def get_team_with_estado(db: AsyncSession, team_id: int) -> Optional[Team]:
    """Busca um time pelo ID com informações do estado"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .where(Team.id == team_id)
    )
    result = await db.execute(stmt)
    team = result.scalar_one_or_none()
    return team

async def list_teams_with_estado(db: AsyncSession) -> List[Team]:
    """Lista todos os times com informações do estado"""
    stmt = (
        select(Team)
        .options(selectinload(Team.estado_obj))
        .order_by(Team.name)
    )
    result = await db.execute(stmt)
    teams = result.scalars().all()
    return teams

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