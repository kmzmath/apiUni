# ranking.py
import math
from datetime import datetime
from typing import List, Dict, Any
import logging

import numpy as np
import pandas as pd
import networkx as nx
import trueskill
from scipy.optimize import minimize
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, distinct

from sqlalchemy.orm import selectinload
from models import Team, Match, TeamMatchInfo

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
    """Classe do código original para rating Bayesiano"""
    def __init__(self, m=PRIOR_MEAN, v=PRIOR_VARIANCE):
        self.prior_mean, self.prior_variance = m, v
    
    def update(self, obs, n_games):
        pw = 1 / (1 + n_games/10)
        mu = pw*self.prior_mean + (1-pw)*obs
        var = self.prior_variance / (1 + n_games/5)
        return mu, math.sqrt(var)


class RankingCalculator:
    def __init__(self, teams: List[Team], matches: List[Match]):
        self.teams = teams
        self.matches = matches
        
        # Prepara DataFrame com validação de duplicatas
        self.matches_df = self._prepare_matches_dataframe()
        
        if len(self.matches_df) == 0:
            raise ValueError("Nenhuma partida válida encontrada para calcular ranking")
        
        # Define times e índices
        self.all_teams = sorted(set(self.matches_df["team_i"]).union(self.matches_df["team_j"]))
        self.n = len(self.all_teams)
        self.team_to_idx = {t: i for i, t in enumerate(self.all_teams)}
        self.idx_to_team = {i: t for t, i in self.team_to_idx.items()}
        
        logger.info(f"✔️ Total de equipes para processamento: {self.n}")
        logger.info(f"✔️ Total de partidas válidas: {len(self.matches_df)}")
        
    def _prepare_matches_dataframe(self) -> pd.DataFrame:
        """Converte matches do banco em DataFrame, removendo duplicatas"""
        data = []
        seen_matches = set()
        
        for match in self.matches:
            # Validações básicas
            if not match.tmi_a or not match.tmi_b:
                continue
            if not match.tmi_a.team or not match.tmi_b.team:
                continue
            if match.tmi_a.score is None or match.tmi_b.score is None:
                continue
            
            team_i_name = match.tmi_a.team.name.strip()
            team_j_name = match.tmi_b.team.name.strip()
            
            # Evita time contra si mesmo
            if team_i_name == team_j_name:
                continue

            match_dt = datetime.combine(match.date, match.time)

            key = tuple(sorted([
                match.tmi_a.team.name.strip(),
                match.tmi_b.team.name.strip()
            ]) + [
                match_dt.strftime("%Y-%m-%d %H:%M"),
                match.mapa
            ])

            # —―― evita registrar a mesma partida duas vezes ―――
            if key in seen_matches:        # já processada
                continue                   # pula duplicata
            seen_matches.add(key)          # registra chave única

            data.append({
                "team_i":  team_i_name,
                "team_j":  team_j_name,
                "score_i": int(match.tmi_a.score),
                "score_j": int(match.tmi_b.score),
                "datetime": match_dt,
                "mapa":     match.mapa,
                "time":     match.time.strftime("%H:%M:%S")   # horário correto
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        # Adiciona colunas auxiliares
        df["idx_i"] = df["team_i"].map(lambda x: None)  # Será preenchido depois
        df["idx_j"] = df["team_j"].map(lambda x: None)
        df["res_i"] = (df["score_i"] > df["score_j"]).astype(int)
        df["res_j"] = 1 - df["res_i"]
        df["margin"] = (df["score_i"] - df["score_j"]).abs()
        df["total_score"] = df["score_i"] + df["score_j"]
        
        # Decaimento temporal
        print("⏰ Aplicando decaimento temporal…")
        latest_dt = df["datetime"].max()
        days_old = (latest_dt - df["datetime"]).dt.total_seconds() / 86_400
        df["time_weight"] = 0.5 ** (days_old / TIME_DECAY_DAYS)
        
        return df.sort_values("datetime").reset_index(drop=True)
    
    def advanced_margin_adjustment(self, margin, total_score):
        """Função do código original"""
        if total_score > 0:
            relative_margin = margin / total_score
        else:
            relative_margin = 0
        adjusted = 2 * np.arctan(relative_margin * 2) / np.pi
        score_factor = 1 + 0.1 * np.log1p(total_score)
        return adjusted * score_factor
    
    def calculate_sos(self, rating_dict):
        """Strength of Schedule do código original"""
        sos = {}
        for t in self.all_teams:
            rows = self.matches_df[(self.matches_df["team_i"] == t) | (self.matches_df["team_j"] == t)]
            opp = np.where(rows["team_i"] == t, rows["team_j"], rows["team_i"])
            vals = [rating_dict[o] for o in opp]
            weights = rows["time_weight"].values
            sos[t] = np.average(vals, weights=weights) if len(vals) else 0
        m, s = np.mean(list(sos.values())), np.std(list(sos.values())) or 1
        return {k: (v-m)/s for k,v in sos.items()}
    
    def consistency_score(self, team):
        """Consistência do código original"""
        tm = self.matches_df[(self.matches_df["team_i"] == team) | (self.matches_df["team_j"] == team)].sort_values("datetime")
        if len(tm) < 5: return 1.0
        w_size = min(5, len(tm)//2)
        perf = []
        for i in range(len(tm)-w_size+1):
            win, diff = 0, 0
            for _, m in tm.iloc[i:i+w_size].iterrows():
                if m["team_i"] == team:
                    win += m["res_i"]; diff += m["score_i"] - m["score_j"]
                else:
                    win += m["res_j"]; diff += m["score_j"] - m["score_i"]
            perf.append(win/w_size + 0.01*diff)
        return 1/(1+np.std(perf)) if len(perf)>1 else 1.0
    
    def calculate_colley(self):
        """Calcula rating Colley (mantendo lógica original)"""
        print("🏗️ Calculando Colley…")
        n = self.n
        G = np.zeros(n); W = np.zeros(n); L = np.zeros(n)
        N_mat = np.zeros((n, n))
        
        for _, r in self.matches_df.iterrows():
            i = self.team_to_idx.get(r.team_i)
            j = self.team_to_idx.get(r.team_j)
            if i is None or j is None:
                continue
            
            w = r.time_weight
            G[i] += w; G[j] += w
            if r.res_i: 
                W[i] += w; L[j] += w
            else:       
                W[j] += w; L[i] += w
            N_mat[i, j] += w; N_mat[j, i] += w
        
        C = np.zeros((n, n))
        for i in range(n):
            C[i, i] = 2 + G[i]
            C[i] -= N_mat[i]
        C[np.diag_indices_from(C)] += N_mat.sum(axis=1)
        
        b = 1 + (W - L) / 2
        r_colley = np.linalg.solve(C, b)
        
        return r_colley
    
    def calculate_massey(self):
        """Calcula rating Massey (mantendo lógica original)"""
        print("🏗️ Calculando Massey…")
        n = self.n
        G = np.zeros(n)
        N_mat = np.zeros((n, n))
        y = np.zeros(n)
        
        for _, r in self.matches_df.iterrows():
            i = self.team_to_idx.get(r.team_i)
            j = self.team_to_idx.get(r.team_j)
            if i is None or j is None:
                continue
            
            w = r.time_weight
            G[i] += w; G[j] += w
            diff = self.advanced_margin_adjustment(r.margin, r.total_score)
            diff = diff if r.res_i else -diff
            y[i] += diff * w; y[j] -= diff * w
            N_mat[i, j] += w; N_mat[j, i] += w
        
        M = np.zeros((n, n))
        for i in range(n):
            M[i, i] = G[i]
            M[i] -= N_mat[i]
        M_prime, y_prime = M.copy(), y.copy()
        M_prime[-1] = 1; y_prime[-1] = 0
        r_massey, *_ = np.linalg.lstsq(M_prime, y_prime, rcond=None)
        
        return r_massey
    
    def calculate_elo(self):
        """Calcula ratings Elo (mantendo lógica original com Bayesian)"""
        print("🏗️ Calculando Elo…")
        
        # Seed com Colley
        r_colley = self.calculate_colley()
        mean_c, std_c = r_colley.mean(), r_colley.std(ddof=0) or 1
        elo_seed = 1500 + (r_colley - mean_c) / std_c * SEED_ELO_STD
        
        def dynamic_K(Ri, Rj, K0=K_ELO_BASE, C=ELO_C_FACTOR):
            diff = abs(Ri - Rj)
            return K0 * diff/(C+diff) if diff else K0
        
        def run_elo(use_mov=False):
            ratings = elo_seed.copy()
            games = np.zeros(self.n)
            
            for _, r in self.matches_df.sort_values("datetime").iterrows():
                i = self.team_to_idx.get(r.team_i)
                j = self.team_to_idx.get(r.team_j)
                if i is None or j is None:
                    continue
                
                games[i] += 1; games[j] += 1
                Ri, Rj = ratings[i], ratings[j]
                Ei = 1/(1+10**((Rj-Ri)/400)); Ej = 1-Ei
                mult = self.advanced_margin_adjustment(r.margin, r.total_score) if use_mov else 1.0
                mult *= r.time_weight
                ratings[i] += dynamic_K(Ri,Rj)*mult*(r.res_i - Ei)
                ratings[j] += dynamic_K(Rj,Ri)*mult*(r.res_j - Ej)
            
            # Bayesian adjustment
            bayes = BayesianRating()
            for i in range(self.n):
                ratings[i], _ = bayes.update(ratings[i], games[i])
            
            return ratings, games
        
        r_elo_final, games_count = run_elo(False)
        r_elo_mov, _ = run_elo(True)
        
        return r_elo_final, r_elo_mov, games_count
    
    def calculate_trueskill(self):
        """Calcula ratings TrueSkill (mantendo lógica original)"""
        print("🏗️ Calculando TrueSkill…")
        ts_env = trueskill.TrueSkill(draw_probability=0)
        ts_ratings = {t: ts_env.create_rating() for t in self.all_teams}
        
        for _, r in self.matches_df.sort_values("datetime").iterrows():
            ti, tj = r.team_i, r.team_j
            Ri, Rj = ts_ratings[ti], ts_ratings[tj]
            new_Ri, new_Rj = (ts_env.rate_1vs1(Ri, Rj)
                              if r.res_i else ts_env.rate_1vs1(Rj, Ri)[::-1])
            w = r.time_weight
            ts_ratings[ti] = trueskill.Rating(Ri.mu*(1-w)+new_Ri.mu*w,
                                              Ri.sigma*(1-w)+new_Ri.sigma*w)
            ts_ratings[tj] = trueskill.Rating(Rj.mu*(1-w)+new_Rj.mu*w,
                                              Rj.sigma*(1-w)+new_Rj.sigma*w)
        
        ts_score = np.array([
            ts_ratings[self.idx_to_team[i]].mu - 3*ts_ratings[self.idx_to_team[i]].sigma
            for i in range(self.n)
        ])
        
        return ts_score
    
    def calculate_pagerank(self):
        """Calcula PageRank (mantendo lógica original)"""
        print("🏗️ Calculando PageRank…")
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
        
        pr_dict = nx.pagerank(G_pr, alpha=DAMPING_PAGERANK, weight="weight")
        r_pagerank = np.array([pr_dict[t] for t in self.all_teams])
        
        return r_pagerank
    
    def calculate_bradley_terry_poisson(self):
        """Calcula Bradley-Terry-Poisson (mantendo lógica original)"""
        print("🏗️ Calculando Bradley-Terry-Poisson…")
        pairwise = []
        for _, r in self.matches_df.iterrows():
            i = self.team_to_idx.get(r.team_i)
            j = self.team_to_idx.get(r.team_j)
            if i is not None and j is not None:
                pairwise.append((i, j, r.score_i, r.score_j, r.time_weight))
        
        def nll_poisson(beta_free):
            beta = np.r_[0.0, beta_free]
            ll = 0.0
            for i,j,si,sj,w in pairwise:
                diff = beta[i] - beta[j]
                lam_i, lam_j = math.exp(diff), math.exp(-diff)
                ll += w*(si*math.log(lam_i) - lam_i - math.lgamma(si+1))
                ll += w*(sj*math.log(lam_j) - lam_j - math.lgamma(sj+1))
            return -ll
        
        opt = minimize(nll_poisson, np.zeros(self.n-1), method="BFGS")
        r_bt_poisson = np.r_[0.0, opt.x]
        
        return r_bt_poisson
    
    def calculate_final_ranking(self) -> pd.DataFrame:
        """Calcula o ranking final (MANTENDO A LÓGICA EXATA DO ORIGINAL)"""
        
        # Calcula todos os métodos base
        r_colley = self.calculate_colley()
        r_massey = self.calculate_massey()
        r_elo_final, r_elo_mov, games_count = self.calculate_elo()
        ts_score = self.calculate_trueskill()
        r_pagerank = self.calculate_pagerank()
        r_bt_poisson = self.calculate_bradley_terry_poisson()
        
        # Cria DataFrame combinado
        combined = pd.DataFrame({
            "team": self.all_teams,
            "r_colley": r_colley,
            "r_massey": r_massey,
            "r_elo_final": r_elo_final,
            "r_elo_mov": r_elo_mov,
            "ts_score": ts_score,
            "r_pagerank": r_pagerank,
            "r_bt_pois": r_bt_poisson,
            "games_count": [games_count[self.team_to_idx[t]] for t in self.all_teams]
        })
        
        # Métricas avançadas
        print("🔧 Integrando métricas avançadas…")
        sos = self.calculate_sos(dict(zip(combined.team, combined.r_elo_final)))
        combined["sos_score"] = combined.team.map(sos)
        combined["consistency"] = combined.team.apply(self.consistency_score)
        
        # Normalização e posições
        methods = ["r_colley","r_massey","r_elo_final","r_elo_mov","ts_score","r_pagerank","r_bt_pois"]

        for m in methods:
            std = combined[m].std(ddof=0) or 1
            combined[f"{m}_z"] = (combined[m]-combined[m].mean())/std
            combined[f"pos_{m}"] = combined[m].rank(ascending=False,method="min").astype(int)

        combined["borda_score"] = 0
        for m in methods:
            combined["borda_score"] += (self.n - combined[f"pos_{m}"] + 1)
                
        # PCA
        print("🔬 Calculando PCA…")
        z = combined[[f"{m}_z" for m in methods]].values
        pca = PCA(n_components=3)
        combined["pca_score"] = pca.fit_transform(z)[:,0]
        
        # Rating final
        print("🎯 Calculando rating final…")
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
        
        # Ajuste direto
        combined["rating_ajustado"] = combined.rating_integrado
        
        # Nota final 0-100
        base = combined.rating_ajustado
        combined["NOTA_FINAL"] = (100*(base-base.min())/(base.max()-base.min())).round(2)
        
        # Intervalos de confiança
        def confidence(row):
            std = 12/math.sqrt(1+row.games_count/5) * (1.5-0.5*row.consistency)
            return pd.Series({
                "ci_lower": round(max(0,row.NOTA_FINAL-1.96*std),2),
                "ci_upper": round(min(100,row.NOTA_FINAL+1.96*std),2),
                "incerteza": round(std,2)
            })
        combined = pd.concat([combined, combined.apply(confidence, axis=1)], axis=1)
        
        team_info = {}
        for team in self.teams:
            team_info[team.name] = {
                'team_id': team.id,
                'tag'    : team.tag or team.name,
                'org'    : team.org or team.name
            }
        
        # Aplica o mapeamento
        combined["team_id"] = combined["team"].map(lambda t: team_info.get(t, {}).get('team_id'))
        combined["tag"]     = combined["team"].map(lambda t: team_info.get(t, {}).get('tag', t))
        combined["org"]     = combined["team"].map(
             lambda t: team_info.get(t, {}).get('org', None)
        )
        
        # Log de times sem mapeamento
        unmapped = combined[combined["team_id"].isna()]
        if len(unmapped) > 0:
            logger.warning(f"⚠️ {len(unmapped)} times sem team_id:")
            for _, row in unmapped.iterrows():
                logger.warning(f"   - {row['team']} (tag: {row['tag']})")
        
        # Estatísticas finais
        print("\n📈 Estatísticas gerais:")
        print(f"- Times: {self.n}")
        print(f"- Partidas: {len(self.matches_df)}")
        print(f"- Média de jogos/time: {combined.games_count.mean():.1f}")
        print(f"- Desvio-padrão das notas: {combined.NOTA_FINAL.std():.2f}")
        
        return combined


async def calculate_ranking(db: AsyncSession, include_variation: bool = True) -> List[dict[str, Any]]:
    """Função principal para calcular o ranking"""
    try:
        # Busca todos os times
        teams_result = await db.execute(select(Team))
        teams = teams_result.scalars().all()
        logger.info(f"🔄 Total de times no banco: {len(teams)}")
        
        # Busca TODAS as partidas sem distinct() para debugar
        matches_stmt = (
            select(Match)
            .options(
                selectinload(Match.tournament),
                selectinload(Match.tmi_a).selectinload(TeamMatchInfo.team),
                selectinload(Match.tmi_b).selectinload(TeamMatchInfo.team),
            )
            .order_by(Match.date)
        )
        
        matches_result = await db.execute(matches_stmt)
        all_matches = list(matches_result.scalars())
        logger.info(f"📊 Total de partidas brutas no banco: {len(all_matches)}")
        
        # Detecta duplicatas para debug
        match_keys = set()
        unique_matches = []
        duplicates = 0
        
        for match in all_matches:
            if not match.tmi_a or not match.tmi_b or not match.tmi_a.team or not match.tmi_b.team:
                continue
                
            # Cria chave única
            key = tuple(sorted([
                match.tmi_a.team.name.strip(),
                match.tmi_b.team.name.strip()
            ]) + [
                match.date.strftime("%Y-%m-%d %H:%M"),
                match.mapa
            ])
            
            if key in match_keys:
                duplicates += 1
            else:
                match_keys.add(key)
                unique_matches.append(match)
        
        logger.info(f"⚠️ Duplicatas detectadas: {duplicates}")
        logger.info(f"✔️ Partidas únicas: {len(unique_matches)}")
        
        if len(unique_matches) == 0:
            logger.warning("Nenhuma partida válida encontrada")
            return []
        
        # Calcula o ranking com partidas únicas
        calculator = RankingCalculator(teams, unique_matches)
        ranking_df = calculator.calculate_final_ranking()
        
        # Ordena por nota final e reseta índice
        ranking_df = ranking_df.sort_values('NOTA_FINAL', ascending=False).reset_index(drop=True)
        
        # Busca último snapshot para calcular variação
        previous_data = {}  # Agora armazena tanto posição quanto nota
        if include_variation:
            try:
                from models import RankingSnapshot, RankingHistory
                
                # Busca o último snapshot (offset 1 para pegar o penúltimo)
                snapshot_stmt = select(RankingSnapshot).order_by(RankingSnapshot.created_at.desc()).offset(1).limit(1)
                snapshot_result = await db.execute(snapshot_stmt)
                last_snapshot = snapshot_result.scalar_one_or_none()
                
                if last_snapshot:
                    # Busca as posições E notas do último snapshot
                    history_stmt = (
                        select(RankingHistory)
                        .where(RankingHistory.snapshot_id == last_snapshot.id)
                    )
                    history_result = await db.execute(history_stmt)
                    
                    for history_entry in history_result.scalars():
                        previous_data[history_entry.team_id] = {
                            'position': history_entry.position,
                            'nota_final': float(history_entry.nota_final)  # Converte Decimal para float
                        }
                    
                    logger.info(f"📊 Comparando com snapshot #{last_snapshot.id} de {last_snapshot.created_at} ({len(previous_data)} times)")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao buscar snapshot anterior: {e}")
        
        # Converte para formato da API
        result = []
        for idx, row in ranking_df.iterrows():
            # idx agora é garantidamente um inteiro
            position = int(idx) + 1
            
            # Calcula variação de posição e nota, verifica se é novo
            variacao = None
            variacao_nota = None
            is_new = False
            
            if include_variation and pd.notna(row.team_id):
                team_id_int = int(row.team_id)
                if team_id_int in previous_data:
                    # Calcula variação de posição (positivo = subiu, negativo = desceu)
                    posicao_anterior = previous_data[team_id_int]['position']
                    variacao = posicao_anterior - position
                    
                    # Calcula variação de nota (positivo = melhorou, negativo = piorou)
                    nota_anterior = previous_data[team_id_int]['nota_final']
                    nota_atual = float(row.NOTA_FINAL)
                    variacao_nota = round(nota_atual - nota_anterior, 2)
                else:
                    # Time não estava no ranking anterior - é novo!
                    is_new = True
            
            result.append({
                "posicao": position,
                "team_id": int(row.team_id) if pd.notna(row.team_id) else None,
                "team": row.team,
                "tag": row.tag,
                "org": row.org,
                "nota_final": float(row.NOTA_FINAL),
                "ci_lower": float(row.ci_lower),
                "ci_upper": float(row.ci_upper),
                "incerteza": float(row.incerteza),
                "games_count": int(row.games_count),
                "variacao": variacao,
                "variacao_nota": variacao_nota,  # NOVO CAMPO
                "is_new": is_new,
                "scores": {
                    "colley": float(row.r_colley),
                    "massey": float(row.r_massey),
                    "elo": float(row.r_elo_final),
                    "elo_mov": float(row.r_elo_mov),
                    "trueskill": float(row.ts_score),
                    "pagerank": float(row.r_pagerank),
                    "bradley_terry": float(row.r_bt_pois),
                    "pca": float(row.pca_score),
                    "sos": float(row.sos_score),
                    "consistency": float(row.consistency),
                    "borda": int(row.borda_score),
                    "integrado": float(row.rating_integrado)
                },
            })
        
        logger.info(f"🏆 Ranking calculado com sucesso para {len(result)} times")
        return result
        
    except Exception as e:
        logger.error(f"❌ Erro ao calcular ranking: {str(e)}", exc_info=True)
        raise