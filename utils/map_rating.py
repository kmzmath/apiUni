"""app/utils/map_rating.py
Calcula notas avançadas para um time em um mapa específico usando as mesmas
métricas do ranking geral (Colley, Massey, Elo, TrueSkill, PageRank, etc.).
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np


class MapRatingCalculator:
    """
    Calcula ratings avançados para times em mapas específicos.
    O construtor recebe **todas** as partidas já serializadas em dicionário
    (matches) para que as métricas que dependem de adversários funcionem.
    """

    # ─────────────────────────── INIT ────────────────────────────

    def __init__(self, matches: List[Dict], team_id: int, map_name: str) -> None:
        self.matches = [m for m in matches if m["mapa"] == map_name]
        self.team_id = team_id
        self.map_name = map_name
        self.teams_in_map = self._get_unique_teams()

    # ───────────────────── HELPERS BÁSICOS ───────────────────────

    def _get_unique_teams(self) -> List[int]:
        teams = set()
        for m in self.matches:
            teams.update((m["team_i_id"], m["team_j_id"]))
        return list(teams)

    # ───────────────────── MÉTRICAS INDIVIDUAIS ─────────────────

    # 1. Colley
    def calculate_colley_rating(self) -> float:
        wins = losses = 0
        for m in self.matches:
            if m["team_i_id"] == self.team_id:
                wins += m["score_i"] > m["score_j"]
                losses += m["score_i"] < m["score_j"]
            elif m["team_j_id"] == self.team_id:
                wins += m["score_j"] > m["score_i"]
                losses += m["score_j"] < m["score_i"]
        tot = wins + losses
        return 0.5 if tot == 0 else (1 + wins - losses) / (2 + tot)

    # 2. Massey
    def calculate_massey_rating(self) -> float:
        if len(self.teams_in_map) < 3:
            return 0.5
        idx = {t: i for i, t in enumerate(self.teams_in_map)}
        n = len(self.teams_in_map)
        A = np.zeros((n, n))
        b = np.zeros(n)

        for m in self.matches:
            i, j = idx[m["team_i_id"]], idx[m["team_j_id"]]
            A[i, i] += 1
            A[j, j] += 1
            A[i, j] -= 1
            A[j, i] -= 1
            diff = m["score_i"] - m["score_j"]
            b[i] += diff
            b[j] -= diff

        A[-1, :] = 1
        b[-1] = 0
        try:
            r = np.linalg.lstsq(A, b, rcond=None)[0]
            return (r[idx[self.team_id]] - r.min()) / (r.max() - r.min() + 1e-6)
        except Exception:
            return 0.5

    # 3. Elo e Elo‑MOV
    def calculate_elo_ratings(self) -> Tuple[float, float]:
        elo, elo_mov = defaultdict(lambda: 1500), defaultdict(lambda: 1500)
        k = 32
        for m in sorted(self.matches, key=lambda x: x["date"]):
            i, j = m["team_i_id"], m["team_j_id"]
            ei = 1 / (1 + 10 ** ((elo[j] - elo[i]) / 400))
            ej = 1 - ei
            if m["score_i"] > m["score_j"]:
                ai, aj = 1, 0
            elif m["score_j"] > m["score_i"]:
                ai, aj = 0, 1
            else:
                ai = aj = 0.5
            elo[i] += k * (ai - ei)
            elo[j] += k * (aj - ej)

            margin = abs(m["score_i"] - m["score_j"])
            mult = math.log(margin + 1) * 2.2 / ((elo[i] - elo[j]) * 0.001 + 2.2)
            elo_mov[i] += k * mult * (ai - ei)
            elo_mov[j] += k * mult * (aj - ej)

        def _norm(d, team):
            vals = list(d.values())
            return (d[team] - min(vals)) / (max(vals) - min(vals) + 1e-6)

        return _norm(elo, self.team_id), _norm(elo_mov, self.team_id)

    # 4. TrueSkill simplificado
    def calculate_trueskill_score(self) -> float:
        perfs = []
        for m in self.matches:
            if m["team_i_id"] == self.team_id:
                my, opp = m["score_i"], m["score_j"]
            elif m["team_j_id"] == self.team_id:
                my, opp = m["score_j"], m["score_i"]
            else:
                continue
            tot = my + opp
            if tot:
                perfs.append(my / tot)
        if not perfs:
            return 0.5
        mu, sigma = np.mean(perfs), np.std(perfs)
        return max(0, min(1, mu - sigma / 2))

    # 5. PageRank
    def calculate_pagerank(self) -> float:
        if len(self.teams_in_map) < 2:
            return 0.5
        idx = {t: i for i, t in enumerate(self.teams_in_map)}
        n = len(self.teams_in_map)
        M = np.zeros((n, n))
        for m in self.matches:
            i, j = idx[m["team_i_id"]], idx[m["team_j_id"]]
            if m["score_i"] > m["score_j"]:
                M[j, i] += 1
            elif m["score_j"] > m["score_i"]:
                M[i, j] += 1
        col_sum = M.sum(0)
        col_sum[col_sum == 0] = 1
        M /= col_sum
        d = 0.85
        P = d * M + (1 - d) * np.ones((n, n)) / n
        try:
            ev, evec = np.linalg.eig(P)
            pr = np.abs(evec[:, np.argmax(ev.real)])
            pr /= pr.sum()
            return pr[idx[self.team_id]]
        except Exception:
            return 1 / n

    # 6. Bradley‑Terry Poisson
    def calculate_bradley_terry_poisson(self) -> float:
        if len(self.teams_in_map) < 2:
            return 0.5
        idx = {t: i for i, t in enumerate(self.teams_in_map)}
        n = len(self.teams_in_map)
        r = np.ones(n)
        for _ in range(100):
            num = np.zeros(n)
            den = np.zeros(n)
            for m in self.matches:
                i, j = idx[m["team_i_id"]], idx[m["team_j_id"]]
                lam_ij = r[i] / (r[i] + r[j])
                lam_ji = r[j] / (r[i] + r[j])
                num[i] += m["score_i"]
                num[j] += m["score_j"]
                tot = m["score_i"] + m["score_j"]
                den[i] += tot * lam_ij
                den[j] += tot * lam_ji
            den[den == 0] = 1
            r = num / den
            r = r / r.sum() * n
        return r[idx[self.team_id]] / r.max()

    # 7. Strength of Schedule
    def calculate_sos_score(self) -> float:
        rec = defaultdict(lambda: {"w": 0, "l": 0})
        opps = []
        for m in self.matches:
            wi = m["score_i"] > m["score_j"]
            wj = m["score_j"] > m["score_i"]
            rec[m["team_i_id"]]["w" if wi else "l"] += 1
            rec[m["team_j_id"]]["w" if wj else "l"] += 1
            if self.team_id == m["team_i_id"]:
                opps.append(m["team_j_id"])
            elif self.team_id == m["team_j_id"]:
                opps.append(m["team_i_id"])
        if not opps:
            return 0.5
        sos = []
        for o in opps:
            g = rec[o]["w"] + rec[o]["l"]
            sos.append(rec[o]["w"] / g if g else 0.5)
        return sum(sos) / len(sos)

    # 8. Consistência
    def calculate_consistency(self) -> float:
        diffs = []
        for m in self.matches:
            if self.team_id == m["team_i_id"]:
                diffs.append(m["score_i"] - m["score_j"])
            elif self.team_id == m["team_j_id"]:
                diffs.append(m["score_j"] - m["score_i"])
        if len(diffs) < 2:
            return 0.5
        mu, sigma = np.mean(diffs), np.std(diffs)
        cv = sigma / abs(mu) if abs(mu) else 1
        return 1 / (1 + cv)

    # ─────────────────── COMPOSIÇÃO FINAL ───────────────────────

    def calculate_all_ratings(self) -> Dict[str, float]:
        elo_final, elo_mov = self.calculate_elo_ratings()
        metrics = {
            "r_colley": self.calculate_colley_rating(),
            "r_massey": self.calculate_massey_rating(),
            "r_elo_final": elo_final,
            "r_elo_mov": elo_mov,
            "ts_score": self.calculate_trueskill_score(),
            "r_pagerank": self.calculate_pagerank(),
            "r_bt_pois": self.calculate_bradley_terry_poisson(),
            "sos_score": self.calculate_sos_score(),
            "consistency": self.calculate_consistency(),
        }
        metrics["pca_score"] = np.mean(list(metrics.values()))
        weights = {
            "r_colley": 0.10,
            "r_massey": 0.10,
            "r_elo_final": 0.15,
            "r_elo_mov": 0.15,
            "ts_score": 0.10,
            "r_pagerank": 0.10,
            "r_bt_pois": 0.10,
            "sos_score": 0.10,
            "consistency": 0.05,
            "pca_score": 0.05,
        }
        metrics["final_rating"] = sum(metrics[k] * weights[k] for k in metrics) * 100
        return metrics