# ranking_history.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from models import RankingSnapshot, RankingHistory, Match
from ranking import calculate_ranking

logger = logging.getLogger(__name__)

async def save_ranking_snapshot(db: AsyncSession) -> int:
    """
    Calcula o ranking atual e salva um snapshot no banco.
    Retorna o ID do snapshot criado.
    """
    try:
        # Calcula o ranking atual
        logger.info("🔄 Calculando ranking para snapshot...")
        ranking_data = await calculate_ranking(db, include_variation=False)
        
        if not ranking_data:
            logger.warning("Nenhum dado de ranking para salvar")
            return None
        
        # Conta total de partidas
        total_matches = await db.execute(select(func.count(Match.id)))
        match_count = total_matches.scalar() or 0
        
        # Cria o snapshot
        snapshot = RankingSnapshot(
            total_matches=match_count,
            total_teams=len(ranking_data),
            snapshot_metadata={
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "version": "1.0",
                "algorithms_used": [
                    "colley", "massey", "elo", "trueskill", 
                    "pagerank", "bradley_terry", "pca"
                ]
            }
        )
        db.add(snapshot)
        await db.flush()  # Para obter o ID
        
        # Salva o histórico de cada time
        for ranking_item in ranking_data:
            if ranking_item["team_id"] is None:
                logger.warning(f"⚠️ Time '{ranking_item['team']}' sem team_id, pulando snapshot")
                continue
                
            history_entry = RankingHistory(
                snapshot_id=snapshot.id,
                team_id=ranking_item["team_id"],
                position=ranking_item["posicao"],
                nota_final=ranking_item["nota_final"],
                ci_lower=ranking_item["ci_lower"],
                ci_upper=ranking_item["ci_upper"],
                incerteza=ranking_item["incerteza"],
                games_count=ranking_item["games_count"],
                
                # Scores individuais
                score_colley=ranking_item["scores"]["colley"],
                score_massey=ranking_item["scores"]["massey"],
                score_elo_final=ranking_item["scores"]["elo"],
                score_elo_mov=ranking_item["scores"]["elo_mov"],
                score_trueskill=ranking_item["scores"]["trueskill"],
                score_pagerank=ranking_item["scores"]["pagerank"],
                score_bradley_terry=ranking_item["scores"]["bradley_terry"],
                score_pca=ranking_item["scores"]["pca"],
                score_sos=ranking_item["scores"]["sos"],
                score_consistency=ranking_item["scores"]["consistency"],
                score_integrado=ranking_item["scores"]["integrado"],
            )
            db.add(history_entry)
        
        await db.commit()
        logger.info(f"✅ Snapshot #{snapshot.id} salvo com {len(ranking_data)} times")
        return snapshot.id
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar snapshot: {str(e)}", exc_info=True)
        await db.rollback()
        raise


async def get_team_history(
    db: AsyncSession, 
    team_id: int,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Retorna o histórico de ranking de um time específico com variações.
    """
    stmt = (
        select(RankingHistory, RankingSnapshot)
        .join(RankingSnapshot)
        .where(RankingHistory.team_id == team_id)
        .order_by(RankingSnapshot.created_at.desc())
        .limit(limit)
    )
    
    result = await db.execute(stmt)
    history_data = []
    
    for rh, rs in result:
        history_data.append({
            "date": rs.created_at.isoformat(),
            "position": rh.position,
            "nota_final": float(rh.nota_final),
            "ci_lower": float(rh.ci_lower),
            "ci_upper": float(rh.ci_upper),
            "games_count": rh.games_count,
            "total_teams": rs.total_teams,
            "scores": {
                "colley": float(rh.score_colley) if rh.score_colley else 0,
                "massey": float(rh.score_massey) if rh.score_massey else 0,
                "elo": float(rh.score_elo_final) if rh.score_elo_final else 0,
                "trueskill": float(rh.score_trueskill) if rh.score_trueskill else 0,
                "pagerank": float(rh.score_pagerank) if rh.score_pagerank else 0,
            }
        })
    
    # Calcula variações entre snapshots consecutivos
    if len(history_data) > 1:
        for i in range(len(history_data) - 1):
            current = history_data[i]
            previous = history_data[i + 1]  # Lembrar que está ordenado DESC
            
            # Variação de posição (positivo = subiu, negativo = desceu)
            current["variacao"] = previous["position"] - current["position"]
            
            # Variação de nota (positivo = melhorou, negativo = piorou)
            current["variacao_nota"] = round(current["nota_final"] - previous["nota_final"], 2)
        
        # O último item (mais antigo) não tem variação
        if history_data:
            history_data[-1]["variacao"] = None
            history_data[-1]["variacao_nota"] = None
    
    return history_data


async def compare_snapshots(
    db: AsyncSession,
    snapshot_id_1: int,
    snapshot_id_2: int
) -> Dict[str, Any]:
    """
    Compara dois snapshots específicos e retorna as diferenças.
    """
    
    # Busca os snapshots
    snapshots_stmt = (
        select(RankingSnapshot)
        .where(RankingSnapshot.id.in_([snapshot_id_1, snapshot_id_2]))
    )
    snapshots_result = await db.execute(snapshots_stmt)
    snapshots = {s.id: s for s in snapshots_result.scalars()}
    
    if len(snapshots) != 2:
        raise ValueError("Um ou ambos snapshots não foram encontrados")
    
    snapshot_1 = snapshots[snapshot_id_1]
    snapshot_2 = snapshots[snapshot_id_2]
    
    # Determina qual é o mais antigo e mais recente
    if snapshot_1.created_at > snapshot_2.created_at:
        newer_snapshot, older_snapshot = snapshot_1, snapshot_2
        newer_id, older_id = snapshot_id_1, snapshot_id_2
    else:
        newer_snapshot, older_snapshot = snapshot_2, snapshot_1
        newer_id, older_id = snapshot_id_2, snapshot_id_1
    
    # Busca dados dos snapshots
    stmt = text("""
        SELECT 
            rh.team_id,
            t.name,
            t.tag,
            t.university,
            rh.snapshot_id,
            rh.position,
            rh.nota_final,
            rh.games_count
        FROM ranking_history rh
        JOIN teams t ON rh.team_id = t.id
        WHERE rh.snapshot_id IN (:newer_id, :older_id)
        ORDER BY rh.team_id, rh.snapshot_id
    """)
    
    result = await db.execute(stmt, {"newer_id": newer_id, "older_id": older_id})
    
    # Organiza dados por time
    teams_data = {}
    for row in result:
        team_id = row.team_id
        if team_id not in teams_data:
            teams_data[team_id] = {
                "team_id": team_id,
                "name": row.name,
                "tag": row.tag,
                "university": row.university,
                "newer": None,
                "older": None
            }
        
        snapshot_data = {
            "position": row.position,
            "nota_final": float(row.nota_final),
            "games_count": row.games_count
        }
        
        if row.snapshot_id == newer_id:
            teams_data[team_id]["newer"] = snapshot_data
        else:
            teams_data[team_id]["older"] = snapshot_data
    
    # Calcula comparações
    comparisons = []
    new_teams = []
    dropped_teams = []
    
    for team_id, data in teams_data.items():
        if data["newer"] and data["older"]:
            # Time presente em ambos snapshots
            newer_pos = data["newer"]["position"]
            older_pos = data["older"]["position"]
            newer_nota = data["newer"]["nota_final"]
            older_nota = data["older"]["nota_final"]
            
            variacao_pos = older_pos - newer_pos  # Positivo = subiu
            variacao_nota = newer_nota - older_nota  # Positivo = melhorou
            
            comparisons.append({
                "team_id": team_id,
                "name": data["name"],
                "tag": data["tag"],
                "university": data["university"],
                "older_position": older_pos,
                "newer_position": newer_pos,
                "older_nota": older_nota,
                "newer_nota": newer_nota,
                "variacao_posicao": variacao_pos,
                "variacao_nota": round(variacao_nota, 2),
                "status": "maintained"
            })
        elif data["newer"] and not data["older"]:
            # Time novo no ranking mais recente
            new_teams.append({
                "team_id": team_id,
                "name": data["name"],
                "tag": data["tag"],
                "university": data["university"],
                "position": data["newer"]["position"],
                "nota_final": data["newer"]["nota_final"],
                "games_count": data["newer"]["games_count"],
                "status": "new"
            })
        elif not data["newer"] and data["older"]:
            # Time que saiu do ranking
            dropped_teams.append({
                "team_id": team_id,
                "name": data["name"],
                "tag": data["tag"],
                "university": data["university"],
                "last_position": data["older"]["position"],
                "last_nota": data["older"]["nota_final"],
                "status": "dropped"
            })
    
    # Ordena por maior variação positiva
    comparisons.sort(key=lambda x: x["variacao_posicao"], reverse=True)
    
    # Estatísticas da comparação
    if comparisons:
        variacoes_pos = [c["variacao_posicao"] for c in comparisons]
        variacoes_nota = [c["variacao_nota"] for c in comparisons]
        
        stats = {
            "teams_compared": len(comparisons),
            "new_teams": len(new_teams),
            "dropped_teams": len(dropped_teams),
            "biggest_rise": max(variacoes_pos) if variacoes_pos else 0,
            "biggest_fall": min(variacoes_pos) if variacoes_pos else 0,
            "avg_position_change": round(sum(variacoes_pos) / len(variacoes_pos), 2) if variacoes_pos else 0,
            "biggest_nota_improvement": max(variacoes_nota) if variacoes_nota else 0,
            "biggest_nota_decline": min(variacoes_nota) if variacoes_nota else 0,
            "avg_nota_change": round(sum(variacoes_nota) / len(variacoes_nota), 2) if variacoes_nota else 0
        }
    else:
        stats = {
            "teams_compared": 0,
            "new_teams": len(new_teams),
            "dropped_teams": len(dropped_teams),
            "biggest_rise": 0,
            "biggest_fall": 0,
            "avg_position_change": 0,
            "biggest_nota_improvement": 0,
            "biggest_nota_decline": 0,
            "avg_nota_change": 0
        }
    
    return {
        "comparison_info": {
            "older_snapshot": {
                "id": older_snapshot.id,
                "created_at": older_snapshot.created_at.isoformat(),
                "total_teams": older_snapshot.total_teams,
                "total_matches": older_snapshot.total_matches
            },
            "newer_snapshot": {
                "id": newer_snapshot.id,
                "created_at": newer_snapshot.created_at.isoformat(),
                "total_teams": newer_snapshot.total_teams,
                "total_matches": newer_snapshot.total_matches
            },
            "time_difference_hours": round(
                (newer_snapshot.created_at - older_snapshot.created_at).total_seconds() / 3600, 1
            )
        },
        "statistics": stats,
        "team_comparisons": comparisons,
        "new_teams": new_teams,
        "dropped_teams": dropped_teams
    }


async def get_ranking_evolution_summary(
    db: AsyncSession,
    days_back: int = 30
) -> Dict[str, Any]:
    """
    Retorna um resumo da evolução do ranking nos últimos N dias.
    """
    
    # Busca snapshots dos últimos N dias
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    
    snapshots_stmt = (
        select(RankingSnapshot)
        .where(RankingSnapshot.created_at >= cutoff_date)
        .order_by(RankingSnapshot.created_at.desc())
    )
    
    snapshots_result = await db.execute(snapshots_stmt)
    snapshots = list(snapshots_result.scalars())
    
    if len(snapshots) < 2:
        return {
            "error": f"Não há snapshots suficientes nos últimos {days_back} dias para análise",
            "snapshots_found": len(snapshots)
        }
    
    # Compara o mais recente com o mais antigo do período
    latest_snapshot = snapshots[0]
    oldest_snapshot = snapshots[-1]
    
    comparison = await compare_snapshots(db, latest_snapshot.id, oldest_snapshot.id)
    
    # Adiciona informações específicas do período
    comparison["period_analysis"] = {
        "days_analyzed": days_back,
        "snapshots_in_period": len(snapshots),
        "snapshot_frequency": round(days_back / len(snapshots), 1) if len(snapshots) > 1 else 0,
        "period_start": oldest_snapshot.created_at.isoformat(),
        "period_end": latest_snapshot.created_at.isoformat()
    }
    
    # Top movers do período
    if comparison["team_comparisons"]:
        top_risers = sorted(
            comparison["team_comparisons"], 
            key=lambda x: x["variacao_posicao"], 
            reverse=True
        )[:5]
        
        top_fallers = sorted(
            comparison["team_comparisons"], 
            key=lambda x: x["variacao_posicao"]
        )[:5]
        
        top_nota_improvers = sorted(
            comparison["team_comparisons"], 
            key=lambda x: x["variacao_nota"], 
            reverse=True
        )[:5]
        
        top_nota_decliners = sorted(
            comparison["team_comparisons"], 
            key=lambda x: x["variacao_nota"]
        )[:5]
        
        comparison["top_movers"] = {
            "biggest_risers": top_risers,
            "biggest_fallers": top_fallers,
            "biggest_nota_improvers": top_nota_improvers,
            "biggest_nota_decliners": top_nota_decliners
        }
    
    return comparison