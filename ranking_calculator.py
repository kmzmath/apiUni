# ranking_calculator.py
import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import logging

import numpy as np
import pandas as pd
import networkx as nx
import trueskill
from scipy.optimize import minimize
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload

from models import Team, Match, TeamMatchInfo, RankingSnapshot, RankingHistory

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ════════════════════════ CONFIGURAÇÕES ══════════════════════ #

K_ELO_BASE = 31
ELO_C_FACTOR = 250
ALPHA_PAGERANK = 1.0
DAMPING_PAGERANK = 0.85
SEED_ELO_STD = 200
TIME_DECAY_DAYS = 110

PRIOR_MEAN = 1500
PRIOR_VARIANCE = 185**2

CONTAMINATION = 0.05
MIN_GAMES_FOR_ANOMALY = 5


class BayesianRating:
    """Classe para rating Bayesiano"""
    def __init__(self, m=PRIOR_MEAN, v=PRIOR_VARIANCE):
        self.prior_mean, self.prior_variance = m, v
    
    def update(self, obs, n_games):
        pw = 1 / (1 + n_games/10)
        mu = pw*self.prior_mean + (1-pw)*obs
        var = self.prior_variance / (1 + n_games/5)
        return mu, math.sqrt(var)


class RankingCalculator:
    """Calculadora principal do sistema de ranking"""
    
    def __init__(self, teams: List[Team], matches: List[Match]):
        self.teams = teams
        self.matches = matches
        
        # Inicializar atributos como None primeiro
        self.team_to_idx = None
        self.idx_to_team = None
        self.all_teams = []
        self.n = 0
        
        # Prepara DataFrame
        self.matches_df = self._prepare_matches_dataframe()
        
        if len(self.matches_df) == 0:
            raise ValueError("Nenhuma partida válida encontrada para calcular ranking")
        
        # Define times e índices apenas se houver partidas válidas
        self.all_teams = sorted(set(self.matches_df["team_i"]).union(self.matches_df["team_j"]))
        self.n = len(self.all_teams)
        
        if self.n == 0:
            raise ValueError("Nenhum time encontrado nas partidas")
        
        self.team_to_idx = {t: i for i, t in enumerate(self.all_teams)}
        self.idx_to_team = {i: t for t, i in self.team_to_idx.items()}
        
        logger.info(f"✔️ Total de equipes: {self.n}")
        logger.info(f"✔️ Total de partidas: {len(self.matches_df)}")
        
    def _prepare_matches_dataframe(self) -> pd.DataFrame:
        """Converte matches em DataFrame"""
        data = []
        seen_matches = set()
        
        for match in self.matches:
            # Validações
            if not hasattr(match, 'team_a') or not hasattr(match, 'team_b'):
                continue
            if not match.team_a or not match.team_b:
                continue
            if match.score_i is None or match.score_j is None:
                continue
            
            # IMPORTANTE: Usar apenas uma linha por partida
            # (diferente do Excel que tem 2 linhas com equipe_referente)
            match_key = match.idPartida
            if match_key in seen_matches:
                continue
            seen_matches.add(match_key)
            
            # Criar linha da partida
            data.append({
                'idPartida': match.idPartida,
                'datetime': match.datetime if hasattr(match, 'datetime') else 
                           datetime.combine(match.date, match.time).replace(tzinfo=timezone.utc),
                'team_i': match.team_a.slug,
                'team_j': match.team_b.slug,
                'score_i': match.score_i,
                'score_j': match.score_j,
                'mapa': match.mapa,
                'team_name_i': match.team_a.name,
                'team_name_j': match.team_b.name
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        # Ordenar por data
        df = df.sort_values('datetime').reset_index(drop=True)
        
        # Adicionar colunas auxiliares
        df["idx_i"] = df["team_i"].map(lambda x: self.team_to_idx.get(x))
        df["idx_j"] = df["team_j"].map(lambda x: self.team_to_idx.get(x))
        df["res_i"] = (df["score_i"] > df["score_j"]).astype(int)
        df["res_j"] = 1 - df["res_i"]
        df["margin"] = (df["score_i"] - df["score_j"]).abs()
        df["total_score"] = df["score_i"] + df["score_j"]
        
        # Decaimento temporal
        latest_dt = df["datetime"].max()
        days_old = (latest_dt - df["datetime"]).dt.total_seconds() / 86_400
        df["time_weight"] = 0.5 ** (days_old / TIME_DECAY_DAYS)
        
        return df
    
    def advanced_margin_adjustment(self, margin, total_score):
        """Ajuste avançado de margem"""
        if total_score > 0:
            relative_margin = margin / total_score
        else:
            relative_margin = 0
        adjusted = 2 * np.arctan(relative_margin * 2) / np.pi
        score_factor = 1 + 0.1 * np.log1p(total_score)
        return adjusted * score_factor
    
    def calculate_colley(self) -> np.ndarray:
        """Calcula ratings Colley"""
        G = np.zeros(self.n)
        W = np.zeros(self.n)
        L = np.zeros(self.n)
        N_mat = np.zeros((self.n, self.n))
        
        for _, r in self.matches_df.iterrows():
            if pd.isna(r.idx_i) or pd.isna(r.idx_j):
                continue
                
            i, j, w = int(r.idx_i), int(r.idx_j), r.time_weight
            G[i] += w
            G[j] += w
            
            if r.res_i:
                W[i] += w
                L[j] += w
            else:
                W[j] += w
                L[i] += w
                
            N_mat[i, j] += w
            N_mat[j, i] += w
        
        C = np.zeros((self.n, self.n))
        for i in range(self.n):
            C[i, i] = 2 + G[i]
            C[i] -= N_mat[i]
        C[np.diag_indices_from(C)] += N_mat.sum(axis=1)
        
        b = 1 + (W - L) / 2
        
        try:
            r_colley = np.linalg.solve(C, b)
        except:
            # Fallback para método iterativo
            r_colley = np.ones(self.n) * 0.5
            
        return r_colley
    
    def calculate_massey(self) -> np.ndarray:
        """Calcula ratings Massey"""
        G = np.zeros(self.n)
        N_mat = np.zeros((self.n, self.n))
        y = np.zeros(self.n)
        
        for _, r in self.matches_df.iterrows():
            if pd.isna(r.idx_i) or pd.isna(r.idx_j):
                continue
                
            i, j, w = int(r.idx_i), int(r.idx_j), r.time_weight
            G[i] += w
            G[j] += w
            
            diff = self.advanced_margin_adjustment(r.margin, r.total_score)
            diff = diff if r.res_i else -diff
            
            y[i] += diff * w
            y[j] -= diff * w
            
            N_mat[i, j] += w
            N_mat[j, i] += w
        
        M = np.zeros((self.n, self.n))
        for i in range(self.n):
            M[i, i] = G[i]
            M[i] -= N_mat[i]
            
        M_prime = M.copy()
        y_prime = y.copy()
        M_prime[-1] = 1
        y_prime[-1] = 0
        
        try:
            r_massey, *_ = np.linalg.lstsq(M_prime, y_prime, rcond=None)
        except:
            r_massey = np.zeros(self.n)
            
        return r_massey
    
    def calculate_elo(self, use_mov=False) -> Tuple[np.ndarray, np.ndarray]:
        """Calcula ratings Elo"""
        # Seed inicial baseado em Colley
        r_colley = self.calculate_colley()
        mean_c, std_c = r_colley.mean(), r_colley.std(ddof=0) or 1
        elo_seed = 1500 + (r_colley - mean_c) / std_c * SEED_ELO_STD
        
        ratings = elo_seed.copy()
        games = np.zeros(self.n)
        
        def dynamic_K(Ri, Rj, K0=K_ELO_BASE, C=ELO_C_FACTOR):
            diff = abs(Ri - Rj)
            return K0 * diff/(C+diff) if diff else K0
        
        for _, r in self.matches_df.iterrows():
            if pd.isna(r.idx_i) or pd.isna(r.idx_j):
                continue
                
            i, j = int(r.idx_i), int(r.idx_j)
            games[i] += 1
            games[j] += 1
            
            Ri, Rj = ratings[i], ratings[j]
            Ei = 1/(1+10**((Rj-Ri)/400))
            Ej = 1-Ei
            
            mult = self.advanced_margin_adjustment(r.margin, r.total_score) if use_mov else 1.0
            mult *= r.time_weight
            
            ratings[i] += dynamic_K(Ri,Rj)*mult*(r.res_i - Ei)
            ratings[j] += dynamic_K(Rj,Ri)*mult*(r.res_j - Ej)
        
        # Aplicar Bayesiano
        bayes = BayesianRating()
        for i in range(self.n):
            ratings[i], _ = bayes.update(ratings[i], games[i])
            
        return ratings, games
    
    def calculate_trueskill(self) -> np.ndarray:
        """Calcula ratings TrueSkill"""
        ts_env = trueskill.TrueSkill(draw_probability=0)
        ts_ratings = {t: ts_env.create_rating() for t in self.all_teams}
        
        for _, r in self.matches_df.iterrows():
            ti, tj = r.team_i, r.team_j
            if ti not in ts_ratings or tj not in ts_ratings:
                continue
                
            Ri, Rj = ts_ratings[ti], ts_ratings[tj]
            
            if r.res_i:
                new_Ri, new_Rj = ts_env.rate_1vs1(Ri, Rj)
            else:
                new_Rj, new_Ri = ts_env.rate_1vs1(Rj, Ri)
            
            w = r.time_weight
            ts_ratings[ti] = trueskill.Rating(
                Ri.mu*(1-w)+new_Ri.mu*w,
                Ri.sigma*(1-w)+new_Ri.sigma*w
            )
            ts_ratings[tj] = trueskill.Rating(
                Rj.mu*(1-w)+new_Rj.mu*w,
                Rj.sigma*(1-w)+new_Rj.sigma*w
            )
        
        # Converter para array
        ts_score = np.array([
            ts_ratings[self.idx_to_team[i]].mu - 3*ts_ratings[self.idx_to_team[i]].sigma
            for i in range(self.n)
        ])
        
        return ts_score
    
    def calculate_pagerank(self) -> np.ndarray:
        """Calcula PageRank"""
        G_pr = nx.DiGraph()
        G_pr.add_nodes_from(self.all_teams)
        
        for _, r in self.matches_df.iterrows():
            winner = r.team_i if r.res_i else r.team_j
            loser = r.team_j if r.res_i else r.team_i
            
            weight = (1 + ALPHA_PAGERANK*self.advanced_margin_adjustment(r.margin, r.total_score)) * r.time_weight
            
            if G_pr.has_edge(loser, winner):
                G_pr[loser][winner]["weight"] += weight
            else:
                G_pr.add_edge(loser, winner, weight=weight)
        
        try:
            pr_dict = nx.pagerank(G_pr, alpha=DAMPING_PAGERANK, weight="weight")
            r_pagerank = np.array([pr_dict.get(t, 0.001) for t in self.all_teams])
        except:
            r_pagerank = np.ones(self.n) / self.n
            
        return r_pagerank
    
    def calculate_bradley_terry(self) -> np.ndarray:
        """Calcula Bradley-Terry-Poisson"""
        pairwise = []
        
        for _, r in self.matches_df.iterrows():
            if pd.isna(r.idx_i) or pd.isna(r.idx_j):
                continue
            pairwise.append((
                int(r.idx_i), int(r.idx_j), 
                r.score_i, r.score_j, r.time_weight
            ))
        
        def nll_poisson(beta_free):
            beta = np.r_[0.0, beta_free]
            ll = 0.0
            for i,j,si,sj,w in pairwise:
                diff = beta[i] - beta[j]
                lam_i, lam_j = math.exp(diff), math.exp(-diff)
                ll += w*(si*math.log(lam_i) - lam_i - math.lgamma(si+1))
                ll += w*(sj*math.log(lam_j) - lam_j - math.lgamma(sj+1))
            return -ll
        
        try:
            opt = minimize(nll_poisson, np.zeros(self.n-1), method="BFGS")
            r_bt_poisson = np.r_[0.0, opt.x]
        except:
            r_bt_poisson = np.zeros(self.n)
            
        return r_bt_poisson
    
    def calculate_sos(self, rating_dict: Dict[str, float]) -> Dict[str, float]:
        """Calcula Strength of Schedule"""
        sos = {}
        
        for t in self.all_teams:
            rows = self.matches_df[
                (self.matches_df["team_i"] == t) | 
                (self.matches_df["team_j"] == t)
            ]
            
            if len(rows) == 0:
                sos[t] = 0
                continue
                
            opp = np.where(rows["team_i"] == t, rows["team_j"], rows["team_i"])
            vals = [rating_dict.get(o, 0) for o in opp]
            weights = rows["time_weight"].values
            
            sos[t] = np.average(vals, weights=weights) if len(vals) else 0
        
        m = np.mean(list(sos.values()))
        s = np.std(list(sos.values())) or 1
        
        return {k: (v-m)/s for k,v in sos.items()}
    
    def calculate_consistency(self, team: str) -> float:
        """Calcula score de consistência"""
        tm = self.matches_df[
            (self.matches_df["team_i"] == team) | 
            (self.matches_df["team_j"] == team)
        ].sort_values("datetime")
        
        if len(tm) < 5:
            return 1.0
            
        w_size = min(5, len(tm)//2)
        perf = []
        
        for i in range(len(tm)-w_size+1):
            win, diff = 0, 0
            for _, m in tm.iloc[i:i+w_size].iterrows():
                if m["team_i"] == team:
                    win += m["res_i"]
                    diff += m["score_i"] - m["score_j"]
                else:
                    win += m["res_j"]
                    diff += m["score_j"] - m["score_i"]
            perf.append(win/w_size + 0.01*diff)
            
        return 1/(1+np.std(perf)) if len(perf)>1 else 1.0
    
    def calculate_final_ranking(self) -> pd.DataFrame:
        """Calcula o ranking final integrando todos os métodos"""
        logger.info("🏗️ Calculando Colley...")
        r_colley = self.calculate_colley()
        
        logger.info("🏗️ Calculando Massey...")
        r_massey = self.calculate_massey()
        
        logger.info("🏗️ Calculando Elo...")
        r_elo_final, games_count = self.calculate_elo(use_mov=False)
        r_elo_mov, _ = self.calculate_elo(use_mov=True)
        
        logger.info("🏗️ Calculando TrueSkill...")
        ts_score = self.calculate_trueskill()
        
        logger.info("🏗️ Calculando PageRank...")
        r_pagerank = self.calculate_pagerank()
        
        logger.info("🏗️ Calculando Bradley-Terry...")
        r_bt_pois = self.calculate_bradley_terry()
        
        # Criar DataFrame combinado
        combined = pd.DataFrame({
            "team": self.all_teams,
            "r_colley": r_colley,
            "r_massey": r_massey,
            "r_elo_final": r_elo_final,
            "r_elo_mov": r_elo_mov,
            "ts_score": ts_score,
            "r_pagerank": r_pagerank,
            "r_bt_pois": r_bt_pois,
            "games_count": [games_count[self.team_to_idx.get(t, 0)] for t in self.all_teams]
        })
        
        # Métricas avançadas
        logger.info("📊 Calculando métricas avançadas...")
        rating_dict = dict(zip(combined.team, combined.r_elo_final))
        sos = self.calculate_sos(rating_dict)
        combined["sos_score"] = combined.team.map(sos)
        combined["consistency"] = combined.team.apply(self.calculate_consistency)
        
        # Normalização
        methods = ["r_colley","r_massey","r_elo_final","r_elo_mov","ts_score","r_pagerank","r_bt_pois"]
        for m in methods:
            std = combined[m].std(ddof=0) or 1
            combined[f"{m}_z"] = (combined[m]-combined[m].mean())/std
            combined[f"pos_{m}"] = combined[m].rank(ascending=False,method="min").astype(int)
        
        combined["borda_score"] = combined[[f"pos_{m}" for m in methods]].sum(axis=1)
        
        # PCA
        logger.info("🔬 Calculando PCA...")
        z = combined[[f"{m}_z" for m in methods]].values
        pca = PCA(n_components=3)
        combined["pca_score"] = pca.fit_transform(z)[:,0]
        
        # Detecção de anomalias
        logger.info("🔍 Detectando anomalias...")
        combined["is_anomaly"] = False
        combined["anomaly_score"] = 0.0
        
        valid = combined.games_count >= MIN_GAMES_FOR_ANOMALY
        if valid.sum() > MIN_GAMES_FOR_ANOMALY:
            iso = IsolationForest(contamination=CONTAMINATION, random_state=42)
            lab = iso.fit_predict(z[valid])
            sc = iso.score_samples(z[valid])
            combined.loc[valid, "is_anomaly"] = lab==-1
            combined.loc[valid, "anomaly_score"] = sc
        
        # Rating final integrado
        logger.info("🎯 Calculando rating final...")
        w = dict(
            base        = 0.55,
            sos         = 0.17,
            consistency = 0.05,
            pca         = 0.23
        )
        
        combined["rating_integrado"] = (
            w["base"]*combined[[f"{m}_z" for m in methods]].mean(1) +
            w["sos"]*combined.sos_score +
            w["consistency"]*combined.consistency +
            w["pca"]*combined.pca_score
        )
        
        combined["rating_ajustado"] = combined.rating_integrado
        
        # Normalizar para 0-100
        base = combined.rating_ajustado
        combined["NOTA_FINAL"] = (100*(base-base.min())/(base.max()-base.min())).round(2)
        
        # Intervalo de confiança
        def confidence(row):
            std = 12/math.sqrt(1+row.games_count/5) * (1.5-0.5*row.consistency)
            return pd.Series({
                "ci_lower": round(max(0,row.NOTA_FINAL-1.96*std),2),
                "ci_upper": round(min(100,row.NOTA_FINAL+1.96*std),2),
                "incerteza": round(std,2)
            })
        
        combined = pd.concat([combined, combined.apply(confidence, axis=1)], axis=1)
        
        # Adicionar Borda score no resultado final
        combined["score_borda"] = combined.borda_score
        
        # Ordenar por nota final
        combined = combined.sort_values("NOTA_FINAL", ascending=False).reset_index(drop=True)
        combined['position'] = combined.index + 1
        
        return combined


async def calculate_and_save_ranking(db: AsyncSession) -> RankingSnapshot:
    """Calcula o ranking e salva no banco de dados"""
    try:
        # Buscar todos os times
        teams_result = await db.execute(select(Team))
        teams = teams_result.scalars().all()
        
        # Buscar todas as partidas com relacionamentos
        matches_result = await db.execute(
            select(Match)
            .options(
                selectinload(Match.team_a),
                selectinload(Match.team_b),
                selectinload(Match.tournament),
                selectinload(Match.map_obj)
            )
            .order_by(Match.date, Match.time)
        )
        matches = matches_result.scalars().all()
        
        logger.info(f"Calculando ranking com {len(teams)} times e {len(matches)} partidas")
        
        # Calcular ranking
        calculator = RankingCalculator(teams, matches)
        ranking_df = calculator.calculate_final_ranking()
        
        # Criar snapshot
        snapshot = RankingSnapshot(
            total_matches=len(matches),
            total_teams=len(ranking_df),
            snapshot_metadata={
                "calculation_date": datetime.now(timezone.utc).isoformat(),
                "algorithms_used": [
                    "colley", "massey", "elo", "elo_mov", 
                    "trueskill", "pagerank", "bradley_terry", "pca"
                ],
                "version": "2.0"
            }
        )
        db.add(snapshot)
        await db.flush()
        
        # Criar mapa de slug para team_id
        team_map = {team.slug: team.id for team in teams}
        
        # Salvar histórico do ranking
        for _, row in ranking_df.iterrows():
            team_id = team_map.get(row['team'])
            if not team_id:
                logger.warning(f"Time '{row['team']}' não encontrado no banco")
                continue
                
            history = RankingHistory(
                snapshot_id=snapshot.id,
                team_id=team_id,
                position=int(row['position']),
                nota_final=float(row['NOTA_FINAL']),
                ci_lower=float(row['ci_lower']),
                ci_upper=float(row['ci_upper']),
                incerteza=float(row['incerteza']),
                games_count=int(row['games_count']),
                score_colley=float(row['r_colley']),
                score_massey=float(row['r_massey']),
                score_elo_final=float(row['r_elo_final']),
                score_elo_mov=float(row['r_elo_mov']),
                score_trueskill=float(row['ts_score']),
                score_pagerank=float(row['r_pagerank']),
                score_bradley_terry=float(row['r_bt_pois']),
                score_pca=float(row['pca_score']),
                score_sos=float(row['sos_score']),
                score_consistency=float(row['consistency']),
                score_integrado=float(row['rating_integrado']),
                score_borda=float(row.get('score_borda', 0))
            )
            db.add(history)
        
        await db.commit()
        logger.info(f"Snapshot #{snapshot.id} salvo com sucesso")
        
        return snapshot
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Erro ao calcular ranking: {str(e)}")
        raise