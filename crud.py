# crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.orm import selectinload
from typing import List, Optional
import uuid
import logging

from models import Team, Tournament, Match, TeamMatchInfo, TeamPlayer, Estado
import schemas

# Configurar logging
logger = logging.getLogger(__name__)

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

async def get_team_map_stats(db: AsyncSession, team_id: int) -> dict:
    """
    Retorna estatísticas detalhadas de mapas para uma equipe específica
    """
    try:
        # Query principal para buscar todas as partidas e estatísticas por mapa
        stmt = text("""
            WITH team_matches AS (
                SELECT 
                    m.id as match_id,
                    m.date,
                    m.map,
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
                    CASE 
                        WHEN tmi.id = m.team_match_info_a THEN t_b.tag
                        ELSE t_a.tag
                    END as opponent_tag,
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
                WHERE m.map IS NOT NULL
            ),
            map_stats AS (
                SELECT 
                    map,
                    COUNT(*) as matches_played,
                    COUNT(CASE WHEN team_score > opponent_score THEN 1 END) as wins,
                    COUNT(CASE WHEN team_score < opponent_score THEN 1 END) as losses,
                    COUNT(CASE WHEN team_score = opponent_score THEN 1 END) as draws,
                    SUM(team_score) as total_rounds_won,
                    SUM(opponent_score) as total_rounds_lost,
                    SUM(team_score + opponent_score) as total_rounds,
                    AVG(team_score) as avg_rounds_won,
                    AVG(opponent_score) as avg_rounds_lost,
                    MAX(CASE WHEN team_score > opponent_score THEN team_score - opponent_score ELSE 0 END) as biggest_win_margin,
                    MAX(CASE WHEN team_score < opponent_score THEN opponent_score - team_score ELSE 0 END) as biggest_loss_margin,
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
                ts.total_matches,
                ts.total_wins,
                ts.total_losses,
                ts.total_draws,
                (ms.matches_played::float / ts.total_matches * 100) as playrate
            FROM map_stats ms
            CROSS JOIN total_stats ts
            ORDER BY ms.matches_played DESC, ms.map
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
                    "overall_winrate": 0.0
                },
                "maps": []
            }
        
        # Extrai estatísticas gerais da primeira linha
        first_row = rows[0]
        overall_stats = {
            "total_matches": first_row.total_matches,
            "total_wins": first_row.total_wins,
            "total_losses": first_row.total_losses,
            "total_draws": first_row.total_draws,
            "total_maps_played": first_row.total_maps_played,
            "overall_winrate": round((first_row.total_wins / first_row.total_matches * 100) if first_row.total_matches > 0 else 0, 2)
        }
        
        # Processa estatísticas por mapa
        maps_stats = []
        for row in rows:
            total_rounds = row.total_rounds_won + row.total_rounds_lost
            
            map_stat = {
                "map_name": row.map,
                "matches_played": row.matches_played,
                "wins": row.wins,
                "losses": row.losses,
                "draws": row.draws,
                "playrate_percent": round(row.playrate, 2),
                "winrate_percent": round((row.wins / row.matches_played * 100) if row.matches_played > 0 else 0, 2),
                "rounds": {
                    "total_played": total_rounds,
                    "total_won": row.total_rounds_won,
                    "total_lost": row.total_rounds_lost,
                    "avg_won_per_match": round(row.avg_rounds_won, 2),
                    "avg_lost_per_match": round(row.avg_rounds_lost, 2),
                    "round_winrate_percent": round((row.total_rounds_won / total_rounds * 100) if total_rounds > 0 else 0, 2)
                },
                "margins": {
                    "biggest_win": row.biggest_win_margin,
                    "biggest_loss": row.biggest_loss_margin
                },
                "dates": {
                    "first_played": row.first_played.isoformat() if row.first_played else None,
                    "last_played": row.last_played.isoformat() if row.last_played else None
                }
            }
            maps_stats.append(map_stat)
        
        # Busca também as partidas mais recentes por mapa para contexto adicional
        recent_matches_stmt = text("""
            WITH team_matches AS (
                SELECT 
                    m.date,
                    m.map,
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
                    t.name as tournament_name,
                    ROW_NUMBER() OVER (PARTITION BY m.map ORDER BY m.date DESC) as rn
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
                WHERE m.map IS NOT NULL
            )
            SELECT * FROM team_matches WHERE rn <= 3
            ORDER BY map, date DESC
        """)
        
        recent_result = await db.execute(recent_matches_stmt, {"team_id": team_id})
        recent_matches = {}
        
        for row in recent_result:
            if row.map not in recent_matches:
                recent_matches[row.map] = []
            
            recent_matches[row.map].append({
                "date": row.date.isoformat(),
                "opponent": row.opponent_name,
                "score": f"{row.team_score}-{row.opponent_score}",
                "result": "W" if row.team_score > row.opponent_score else ("L" if row.team_score < row.opponent_score else "D"),
                "tournament": row.tournament_name
            })
        
        # Adiciona partidas recentes a cada mapa
        for map_stat in maps_stats:
            map_stat["recent_matches"] = recent_matches.get(map_stat["map_name"], [])
        
        return {
            "team_id": team_id,
            "overall_stats": overall_stats,
            "maps": maps_stats
        }
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas de mapas do time {team_id}: {e}")
        return {
            "team_id": team_id,
            "overall_stats": {
                "total_matches": 0,
                "total_wins": 0,
                "total_losses": 0,
                "total_draws": 0,
                "total_maps_played": 0,
                "overall_winrate": 0.0
            },
            "maps": [],
            "error": str(e)
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