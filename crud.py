from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc, asc
from sqlalchemy.orm import selectinload, joinedload
from typing import List, Optional
import logging
from sqlalchemy import text

from models import (
    Team, Estado, TeamPlayer, Tournament, Match, 
    TeamMatchInfo, RankingSnapshot, RankingHistory
)

logger = logging.getLogger(__name__)

# ===== TEAMS =====

async def list_teams(db: AsyncSession) -> List[Team]:
    """Lista todos os times com informações do estado"""
    try:
        query = (
            select(Team)
            .options(joinedload(Team.estado_obj))
            .order_by(Team.name)
        )
        
        result = await db.execute(query)
        return result.unique().scalars().all()
    except Exception as e:
        logger.error(f"Erro ao listar times: {str(e)}")
        return []

async def get_team_by_slug(db: AsyncSession, slug: str) -> Optional[Team]:
    """Busca um time pelo slug"""
    try:
        query = (
            select(Team)
            .options(joinedload(Team.estado_obj))
            .where(Team.slug == slug)
        )
        
        result = await db.execute(query)
        return result.unique().scalar_one_or_none()
    except Exception as e:
        logger.error(f"Erro ao buscar time por slug: {str(e)}")
        return None

async def get_team(db: AsyncSession, team_id: int) -> Optional[Team]:
    """Busca um time pelo ID"""
    try:
        query = (
            select(Team)
            .options(joinedload(Team.estado_obj))
            .where(Team.id == team_id)
        )
        
        result = await db.execute(query)
        return result.unique().scalar_one_or_none()
    except Exception as e:
        logger.error(f"Erro ao buscar time por ID: {str(e)}")
        return None

# ===== PLAYERS =====

async def get_team_players(db: AsyncSession, team_id: int) -> List[TeamPlayer]:
    """Busca os jogadores de um time"""
    try:
        query = (
            select(TeamPlayer)
            .where(TeamPlayer.team_id == team_id)
            .order_by(TeamPlayer.id)
        )
        
        result = await db.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Erro ao buscar jogadores: {str(e)}")
        return []

# ===== MATCHES =====

async def get_team_matches(db: AsyncSession, team_id: int, limit: int = 50) -> List[Match]:
    """Busca as partidas de um time"""
    try:
        # Primeiro buscar o slug do time
        team = await get_team(db, team_id)
        if not team:
            return []
        
        query = (
            select(Match)
            .options(
                joinedload(Match.tournament_rel),
                joinedload(Match.tmi_a_rel).joinedload(TeamMatchInfo.team).joinedload(Team.estado_obj),
                joinedload(Match.tmi_b_rel).joinedload(TeamMatchInfo.team).joinedload(Team.estado_obj),
                joinedload(Match.team_i_obj).joinedload(Team.estado_obj),
                joinedload(Match.team_j_obj).joinedload(Team.estado_obj)
            )
            .where(or_(
                Match.team_i == team.slug,
                Match.team_j == team.slug
            ))
            .order_by(Match.date.desc(), Match.time.desc())
            .limit(limit)
        )
        
        result = await db.execute(query)
        return result.unique().scalars().all()
    except Exception as e:
        logger.error(f"Erro ao buscar partidas do time: {str(e)}")
        return []

async def list_recent_matches(db: AsyncSession, limit: int = 20) -> List[Match]:
    """Lista as partidas mais recentes"""
    try:
        query = (
            select(Match)
            .options(
                joinedload(Match.tournament_rel),
                joinedload(Match.tmi_a_rel).joinedload(TeamMatchInfo.team).joinedload(Team.estado_obj),
                joinedload(Match.tmi_b_rel).joinedload(TeamMatchInfo.team).joinedload(Team.estado_obj),
                joinedload(Match.team_i_obj).joinedload(Team.estado_obj),
                joinedload(Match.team_j_obj).joinedload(Team.estado_obj)
            )
            .order_by(Match.date.desc(), Match.time.desc())
            .limit(limit)
        )
        
        result = await db.execute(query)
        return result.unique().scalars().all()
    except Exception as e:
        logger.error(f"Erro ao listar partidas: {str(e)}")
        return []

# ===== TOURNAMENTS =====

async def list_tournaments(db: AsyncSession) -> List[Tournament]:
    """Lista todos os torneios"""
    try:
        query = select(Tournament).order_by(Tournament.start_date.desc())
        result = await db.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Erro ao listar torneios: {str(e)}")
        return []

# ===== RANKING =====

async def get_latest_ranking_snapshot(db: AsyncSession) -> Optional[RankingSnapshot]:
    """Busca o snapshot de ranking mais recente"""
    try:
        query = (
            select(RankingSnapshot)
            .order_by(RankingSnapshot.created_at.desc())
            .limit(1)
        )
        
        result = await db.execute(query)
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Erro ao buscar snapshot: {str(e)}")
        return None

async def get_ranking_by_snapshot(
    db: AsyncSession, 
    snapshot_id: int, 
    limit: Optional[int] = None
) -> List[RankingHistory]:
    """Busca o ranking de um snapshot específico"""
    try:
        query = (
            select(RankingHistory)
            .options(joinedload(RankingHistory.team))
            .where(RankingHistory.snapshot_id == snapshot_id)
            .order_by(RankingHistory.position)
        )
        
        if limit:
            query = query.limit(limit)
        
        result = await db.execute(query)
        return result.unique().scalars().all()
    except Exception as e:
        logger.error(f"Erro ao buscar ranking: {str(e)}")
        return []

async def get_ranking_snapshots(
    db: AsyncSession, 
    limit: int = 10
) -> List[RankingSnapshot]:
    """Lista os snapshots de ranking"""
    try:
        query = (
            select(RankingSnapshot)
            .order_by(RankingSnapshot.created_at.desc())
            .limit(limit)
        )
        
        result = await db.execute(query)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Erro ao buscar snapshots: {str(e)}")
        return []
    
async def get_ranking_snapshots_raw(db: AsyncSession, limit: int = 10) -> List[dict]:
    """Lista os snapshots de ranking usando SQL raw (compatível com pgbouncer)"""
    try:
        query = text("""
            SELECT 
                id, 
                created_at, 
                total_matches, 
                total_teams, 
                snapshot_metadata
            FROM ranking_snapshots
            ORDER BY created_at DESC
            LIMIT :limit
        """)
        
        result = await db.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        snapshots = []
        for row in rows:
            snapshots.append({
                "id": row.id,
                "created_at": row.created_at,
                "total_matches": row.total_matches,
                "total_teams": row.total_teams,
                "metadata": row.snapshot_metadata or {}
            })
        
        return snapshots
    except Exception as e:
        logger.error(f"Erro ao buscar snapshots (raw): {str(e)}")
        return []

async def get_ranking_by_snapshot_raw(db: AsyncSession, snapshot_id: int) -> List[dict]:
    """Busca o ranking de um snapshot usando SQL raw"""
    try:
        query = text("""
            SELECT 
                rh.position,
                rh.team_id,
                rh.nota_final,
                rh.ci_lower,
                rh.ci_upper,
                rh.incerteza,
                rh.games_count,
                rh.score_colley,
                rh.score_massey,
                rh.score_elo_final,
                rh.score_elo_mov,
                rh.score_trueskill,
                rh.score_pagerank,
                rh.score_bradley_terry,
                rh.score_pca,
                rh.score_sos,
                rh.score_consistency,
                rh.score_integrado,
                t.name as team_name,
                t.tag as team_tag,
                t.org as team_org
            FROM ranking_history rh
            JOIN teams t ON rh.team_id = t.id
            WHERE rh.snapshot_id = :snapshot_id
            ORDER BY rh.position
        """)
        
        result = await db.execute(query, {"snapshot_id": snapshot_id})
        rows = result.fetchall()
        
        rankings = []
        for row in rows:
            rankings.append({
                "position": row.position,
                "team_id": row.team_id,
                "team_name": row.team_name,
                "team_tag": row.team_tag,
                "team_org": row.team_org,
                "nota_final": float(row.nota_final),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": row.games_count,
                "scores": {
                    "colley": float(row.score_colley or 0),
                    "massey": float(row.score_massey or 0),
                    "elo": float(row.score_elo_final or 0),
                    "elo_mov": float(row.score_elo_mov or 0),
                    "trueskill": float(row.score_trueskill or 0),
                    "pagerank": float(row.score_pagerank or 0),
                    "bradley_terry": float(row.score_bradley_terry or 0),
                    "pca": float(row.score_pca or 0),
                    "sos": float(row.score_sos or 0),
                    "consistency": float(row.score_consistency or 0),
                    "integrado": float(row.score_integrado or 0)
                }
            })
        
        return rankings
    except Exception as e:
        logger.error(f"Erro ao buscar ranking (raw): {str(e)}")
        return []

# Adicione estas funções no crud.py

async def get_previous_ranking_snapshot(db: AsyncSession) -> Optional[RankingSnapshot]:
    """Busca o penúltimo snapshot de ranking para calcular variações"""
    try:
        query = (
            select(RankingSnapshot)
            .order_by(RankingSnapshot.created_at.desc())
            .offset(1)  # Pula o mais recente
            .limit(1)
        )
        
        result = await db.execute(query)
        return result.scalar_one_or_none()
    except Exception as e:
        logger.error(f"Erro ao buscar snapshot anterior: {str(e)}")
        return None

async def get_ranking_with_variations(
    db: AsyncSession, 
    snapshot_id: int, 
    limit: Optional[int] = None
) -> List[dict]:
    """Busca o ranking com cálculo de variações comparando com snapshot anterior"""
    try:
        # Buscar ranking atual
        current_rankings = await get_ranking_by_snapshot(db, snapshot_id, limit)
        
        # Buscar snapshot anterior
        previous_snapshot = await get_previous_ranking_snapshot(db)
        
        # Se não houver snapshot anterior, retornar sem variações
        if not previous_snapshot:
            return [{
                "ranking_history": rank,
                "variacao": 0,
                "variacao_nota": 0.0,
                "is_new": True
            } for rank in current_rankings]
        
        # Buscar ranking anterior
        previous_rankings = await get_ranking_by_snapshot(db, previous_snapshot.id)
        
        # Criar mapa do ranking anterior
        previous_data = {}
        for prev_rank in previous_rankings:
            previous_data[prev_rank.team_id] = {
                'position': prev_rank.position,
                'nota_final': float(prev_rank.nota_final)
            }
        
        # Calcular variações
        result = []
        for rank in current_rankings:
            variacao = 0
            variacao_nota = 0.0
            is_new = False
            
            if rank.team_id in previous_data:
                # Time existia no ranking anterior
                prev_position = previous_data[rank.team_id]['position']
                prev_nota = previous_data[rank.team_id]['nota_final']
                
                variacao = prev_position - rank.position  # Positivo se subiu
                variacao_nota = float(rank.nota_final) - prev_nota
            else:
                # Time novo no ranking
                is_new = True
            
            result.append({
                "ranking_history": rank,
                "variacao": variacao,
                "variacao_nota": round(variacao_nota, 2),
                "is_new": is_new
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Erro ao calcular variações: {str(e)}")
        return []

async def get_team_players_complete(db: AsyncSession, team_id: int) -> List[dict]:
    """
    Busca jogadores de um time tanto da tabela team_players quanto dos campos legacy
    """
    try:
        # Primeiro tenta buscar da tabela team_players
        players_query = (
            select(TeamPlayer)
            .where(TeamPlayer.team_id == team_id)
            .order_by(TeamPlayer.id)
        )
        
        result = await db.execute(players_query)
        players = result.scalars().all()
        
        # Se encontrou jogadores na tabela nova, retorna
        if players:
            return [{"id": p.id, "nick": p.player_nick} for p in players]
        
        # Se não encontrou, busca dos campos legacy na tabela teams
        team = await get_team(db, team_id)
        if not team:
            return []
        
        legacy_players = []
        player_fields = [
            team.player1, team.player2, team.player3, team.player4, team.player5,
            team.player6, team.player7, team.player8, team.player9, team.player10
        ]
        
        for i, player_nick in enumerate(player_fields, 1):
            if player_nick and player_nick.strip():
                legacy_players.append({
                    "id": i,  # ID fictício baseado na posição
                    "nick": player_nick.strip()
                })
        
        return legacy_players
        
    except Exception as e:
        logger.error(f"Erro ao buscar jogadores completos: {str(e)}")
        return []

# Versão alternativa usando SQL raw para melhor performance
async def get_ranking_with_variations_raw(db: AsyncSession, snapshot_id: int) -> List[dict]:
    """Versão otimizada usando SQL raw para calcular variações"""
    try:
        query = text("""
            WITH current_ranking AS (
                SELECT 
                    rh.position,
                    rh.team_id,
                    rh.nota_final,
                    rh.ci_lower,
                    rh.ci_upper,
                    rh.incerteza,
                    rh.games_count,
                    rh.score_colley,
                    rh.score_massey,
                    rh.score_elo_final,
                    rh.score_elo_mov,
                    rh.score_trueskill,
                    rh.score_pagerank,
                    rh.score_bradley_terry,
                    rh.score_pca,
                    rh.score_sos,
                    rh.score_consistency,
                    rh.score_integrado,
                    t.name as team_name,
                    t.tag as team_tag,
                    t.org as team_org
                FROM ranking_history rh
                JOIN teams t ON rh.team_id = t.id
                WHERE rh.snapshot_id = :current_snapshot_id
            ),
            previous_ranking AS (
                SELECT 
                    rh.position as prev_position,
                    rh.team_id,
                    rh.nota_final as prev_nota_final
                FROM ranking_history rh
                WHERE rh.snapshot_id = (
                    SELECT id FROM ranking_snapshots 
                    WHERE id < :current_snapshot_id
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            )
            SELECT 
                cr.*,
                COALESCE(pr.prev_position - cr.position, 0) as variacao,
                COALESCE(cr.nota_final - pr.prev_nota_final, 0) as variacao_nota,
                CASE WHEN pr.team_id IS NULL THEN true ELSE false END as is_new
            FROM current_ranking cr
            LEFT JOIN previous_ranking pr ON cr.team_id = pr.team_id
            ORDER BY cr.position
        """)
        
        result = await db.execute(query, {"current_snapshot_id": snapshot_id})
        rows = result.fetchall()
        
        rankings = []
        for row in rows:
            rankings.append({
                "position": row.position,
                "team_id": row.team_id,
                "team_name": row.team_name,
                "team_tag": row.team_tag,
                "team_org": row.team_org,
                "nota_final": float(row.nota_final),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": row.games_count,
                "variacao": int(row.variacao),
                "variacao_nota": float(row.variacao_nota),
                "is_new": bool(row.is_new),
                "scores": {
                    "colley": float(row.score_colley or 0),
                    "massey": float(row.score_massey or 0),
                    "elo": float(row.score_elo_final or 0),
                    "elo_mov": float(row.score_elo_mov or 0),
                    "trueskill": float(row.score_trueskill or 0),
                    "pagerank": float(row.score_pagerank or 0),
                    "bradley_terry": float(row.score_bradley_terry or 0),
                    "pca": float(row.score_pca or 0),
                    "sos": float(row.score_sos or 0),
                    "consistency": float(row.score_consistency or 0),
                    "integrado": float(row.score_integrado or 0)
                }
            })
        
        return rankings
    except Exception as e:
        logger.error(f"Erro ao buscar ranking com variações (raw): {str(e)}")
        return []

async def get_ranking_with_variations_between_snapshots_raw(
    db: AsyncSession, 
    current_snapshot_id: int,
    previous_snapshot_id: int
) -> List[dict]:
    """Calcula variações entre dois snapshots específicos"""
    try:
        query = text("""
            WITH current_ranking AS (
                SELECT 
                    rh.position,
                    rh.team_id,
                    rh.nota_final,
                    rh.ci_lower,
                    rh.ci_upper,
                    rh.incerteza,
                    rh.games_count,
                    rh.score_colley,
                    rh.score_massey,
                    rh.score_elo_final,
                    rh.score_elo_mov,
                    rh.score_trueskill,
                    rh.score_pagerank,
                    rh.score_bradley_terry,
                    rh.score_pca,
                    rh.score_sos,
                    rh.score_consistency,
                    rh.score_integrado,
                    t.name as team_name,
                    t.tag as team_tag,
                    t.org as team_org
                FROM ranking_history rh
                JOIN teams t ON rh.team_id = t.id
                WHERE rh.snapshot_id = :current_snapshot_id
            ),
            previous_ranking AS (
                SELECT 
                    rh.position as prev_position,
                    rh.team_id,
                    rh.nota_final as prev_nota_final
                FROM ranking_history rh
                WHERE rh.snapshot_id = :previous_snapshot_id
            )
            SELECT 
                cr.*,
                COALESCE(pr.prev_position - cr.position, 0) as variacao,
                COALESCE(cr.nota_final - pr.prev_nota_final, 0) as variacao_nota,
                CASE WHEN pr.team_id IS NULL THEN true ELSE false END as is_new
            FROM current_ranking cr
            LEFT JOIN previous_ranking pr ON cr.team_id = pr.team_id
            ORDER BY cr.position
        """)
        
        result = await db.execute(query, {
            "current_snapshot_id": current_snapshot_id,
            "previous_snapshot_id": previous_snapshot_id
        })
        rows = result.fetchall()
        
        rankings = []
        for row in rows:
            rankings.append({
                "position": row.position,
                "team_id": row.team_id,
                "team_name": row.team_name,
                "team_tag": row.team_tag,
                "team_org": row.team_org,
                "nota_final": float(row.nota_final),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": row.games_count,
                "variacao": int(row.variacao),
                "variacao_nota": round(float(row.variacao_nota), 2),
                "is_new": bool(row.is_new),
                "scores": {
                    "colley": float(row.score_colley or 0),
                    "massey": float(row.score_massey or 0),
                    "elo": float(row.score_elo_final or 0),
                    "elo_mov": float(row.score_elo_mov or 0),
                    "trueskill": float(row.score_trueskill or 0),
                    "pagerank": float(row.score_pagerank or 0),
                    "bradley_terry": float(row.score_bradley_terry or 0),
                    "pca": float(row.score_pca or 0),
                    "sos": float(row.score_sos or 0),
                    "consistency": float(row.score_consistency or 0),
                    "integrado": float(row.score_integrado or 0)
                }
            })
        
        return rankings
    except Exception as e:
        logger.error(f"Erro ao calcular variações entre snapshots: {str(e)}")
        return []