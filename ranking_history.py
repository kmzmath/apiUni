# ranking_history.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timezone
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
                "version": "2.0",
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
                logger.warning(f"⚠️ Time '{ranking_item['team']}' sem team_id, pulando")
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
                score_borda=ranking_item["scores"]["borda"]
            )
            db.add(history_entry)
        
        await db.commit()
        logger.info(f"✅ Snapshot #{snapshot.id} salvo com {len(ranking_data)} times")
        return snapshot.id
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar snapshot: {str(e)}", exc_info=True)
        await db.rollback()
        raise