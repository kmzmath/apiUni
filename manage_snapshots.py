# manage_snapshots.py  –  rev. 2025-07-28
"""
CLI para gerenciar snapshots de ranking diretamente no Supabase.

• Mantém todas as funções originais (capturar, listar, exportar, excluir,
  limpar, testar conexão, ver configs).

• Trabalha diretamente com o banco Supabase ao invés da API REST.

• Cada snapshot completo pode ser salvo em ./snapshots_data/{id}.json
"""

from __future__ import annotations

import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client, Client

# ─────────────────────────── Config e diretórios ─────────────────────────── #

load_dotenv()
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE: str = os.getenv("SUPABASE_SERVICE_ROLE", "")

# Inicializa cliente Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
supabase.postgrest.auth(SUPABASE_SERVICE_ROLE)

SAVE_DIR = Path(__file__).with_name("snapshots_data")
SAVE_DIR.mkdir(exist_ok=True)

# ─────────────────────────── Helpers genéricos ─────────────────────────── #


def clear_console() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def human_diff(created_at: str | datetime) -> str:
    if isinstance(created_at, str):
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    else:
        dt = created_at
    diff = datetime.now(timezone.utc) - dt
    hours = diff.total_seconds() / 3600
    if hours < 1:
        return f"{int(diff.total_seconds() / 60)} min atrás"
    if hours < 24:
        return f"{int(hours)} h atrás"
    return f"{int(hours // 24)} d atrás"


def save_snapshot_file(snapshot_data: Dict[str, Any]) -> None:
    """Salva o snapshot (payload completo) em snapshots_data/{id}.json."""
    sid = snapshot_data.get("id")
    if not sid:
        print("❌ Snapshot sem campo 'id' – não pôde salvar.")
        return

    path = SAVE_DIR / f"{sid}.json"
    with path.open("w", encoding="utf-8") as fp:
        json.dump(snapshot_data, fp, indent=2, ensure_ascii=False, default=str)

    rel = path.relative_to(Path.cwd())
    print(f"💾  Arquivo salvo: {rel}")


# ─────────────────────────── Funções de Ranking ────────────────────────── #


def calculate_team_ranking() -> List[Dict[str, Any]]:
    """
    Calcula o ranking dos times baseado nas partidas.
    """
    # Busca todas as partidas
    matches = supabase.table("matches").select("*").execute().data
    
    # Busca informações dos times incluindo o ID
    teams_data = supabase.table("teams").select("id, slug, name, tag, logo").execute().data
    teams_dict = {t["slug"]: t for t in teams_data}
    
    # Calcula estatísticas por time
    team_stats = defaultdict(lambda: {
        "wins": 0,
        "losses": 0,
        "rounds_won": 0,
        "rounds_lost": 0,
        "matches": []
    })
    
    for match in matches:
        if not match.get("team_i") or not match.get("team_j"):
            continue
            
        score_i = match.get("score_i", 0) or 0
        score_j = match.get("score_j", 0) or 0
        
        # Team i stats
        team_stats[match["team_i"]]["rounds_won"] += score_i
        team_stats[match["team_i"]]["rounds_lost"] += score_j
        team_stats[match["team_i"]]["matches"].append(match["idPartida"])
        
        # Team j stats
        team_stats[match["team_j"]]["rounds_won"] += score_j
        team_stats[match["team_j"]]["rounds_lost"] += score_i
        team_stats[match["team_j"]]["matches"].append(match["idPartida"])
        
        # Vitórias/Derrotas
        if score_i > score_j:
            team_stats[match["team_i"]]["wins"] += 1
            team_stats[match["team_j"]]["losses"] += 1
        elif score_j > score_i:
            team_stats[match["team_j"]]["wins"] += 1
            team_stats[match["team_i"]]["losses"] += 1
    
    # Constrói ranking
    ranking = []
    for team_slug, stats in team_stats.items():
        team_info = teams_dict.get(team_slug, {})
        if not team_info:  # Skip if team not found
            continue
            
        # Calcula pontuação (3 por vitória)
        points = stats["wins"] * 3
        
        # Calcula winrate
        total_matches = stats["wins"] + stats["losses"]
        winrate = (stats["wins"] / total_matches * 100) if total_matches > 0 else 0
        
        # Round difference
        round_diff = stats["rounds_won"] - stats["rounds_lost"]
        
        ranking.append({
            "position": 0,  # Será calculado após ordenação
            "team_id": team_info.get("id"),
            "team_slug": team_slug,
            "team_name": team_info.get("name", team_slug),
            "team_tag": team_info.get("tag", ""),
            "team_logo": team_info.get("logo", ""),
            "points": points,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "matches_played": total_matches,
            "rounds_won": stats["rounds_won"],
            "rounds_lost": stats["rounds_lost"],
            "round_difference": round_diff,
            "winrate": round(winrate, 2),
            "match_ids": stats["matches"]
        })
    
    # Ordena por pontos, depois por round difference
    ranking.sort(key=lambda x: (x["points"], x["round_difference"]), reverse=True)
    
    # Atribui posições
    for i, team in enumerate(ranking):
        team["position"] = i + 1
    
    return ranking


# ─────────────────────── Operações principais ────────────────────────── #


def get_snapshot_with_ranking(snapshot_id: int) -> Dict[str, Any]:
    """Busca um snapshot com seu ranking completo."""
    # Busca snapshot
    snap_result = supabase.table("ranking_snapshots").select("*").eq("id", snapshot_id).execute()
    if not snap_result.data:
        raise ValueError(f"Snapshot {snapshot_id} não encontrado")
    
    snapshot = snap_result.data[0]
    
    # Busca ranking history
    ranking_result = (
        supabase.table("ranking_history")
        .select("*, teams(slug, name, tag, logo)")
        .eq("snapshot_id", snapshot_id)
        .order("position")
        .execute()
    )
    
    # Formata dados do ranking
    ranking_data = []
    for entry in ranking_result.data:
        team_data = entry.get("teams", {})
        ranking_data.append({
            "position": entry["position"],
            "team_id": entry["team_id"],
            "team_slug": team_data.get("slug", ""),
            "team_name": team_data.get("name", ""),
            "team_tag": team_data.get("tag", ""),
            "team_logo": team_data.get("logo", ""),
            "nota_final": float(entry["nota_final"]),
            "games_count": entry["games_count"],
            "ci_lower": float(entry["ci_lower"]),
            "ci_upper": float(entry["ci_upper"]),
            "incerteza": float(entry["incerteza"])
        })
    
    snapshot["ranking_data"] = ranking_data
    return snapshot


def check_latest_snapshot() -> Optional[Dict[str, Any]]:
    """Mostra informações do snapshot mais recente (se existir)."""
    try:
        result = (
            supabase.table("ranking_snapshots")
            .select("id, created_at, total_teams, total_matches")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        
        if not result.data:
            print("\n⚠️  Nenhum snapshot encontrado ainda.")
            return None

        snap = result.data[0]
        print("\n📊 Último Snapshot:")
        print(f"   ID:          #{snap['id']}")
        print(f"   Capturado:   {human_diff(snap['created_at'])}")
        print(f"   Times:       {snap['total_teams']}")
        print(f"   Partidas:    {snap['total_matches']}")
        return snap
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        return None


def capture_new_snapshot() -> None:
    """Captura um novo snapshot do ranking atual."""
    print("\n🔄 Capturando novo snapshot…")
    try:
        # Calcula o ranking atual
        ranking = calculate_team_ranking()
        
        # Conta total de partidas
        matches_count = supabase.table("matches").select("idPartida", count="exact").execute().count
        
        # Metadados do snapshot
        snapshot_metadata = {
            "version": "2.0",
            "source": "supabase",
            "captured_by": "manage_snapshots.py",
            "algorithm": "points_system"  # Sistema de pontos simples
        }
        
        # Cria o snapshot primeiro
        snapshot_data = {
            "total_teams": len(ranking),
            "total_matches": matches_count,
            "snapshot_metadata": snapshot_metadata
        }
        
        snapshot_result = supabase.table("ranking_snapshots").insert(snapshot_data).execute()
        
        if not snapshot_result.data:
            print("\n❌ Erro ao criar snapshot")
            return
            
        snapshot = snapshot_result.data[0]
        snapshot_id = snapshot["id"]
        
        # Insere dados do ranking na ranking_history
        ranking_history_entries = []
        for team in ranking:
            if not team.get("team_id"):  # Skip teams without ID
                continue
                
            entry = {
                "snapshot_id": snapshot_id,
                "team_id": team["team_id"],
                "position": team["position"],
                "nota_final": float(team["points"]),  # Usando pontos como nota_final
                "games_count": team["matches_played"],
                # Valores simplificados para os campos obrigatórios
                "ci_lower": float(team["points"] - 5),  # Intervalo de confiança fictício
                "ci_upper": float(team["points"] + 5),
                "incerteza": 5.0,  # Incerteza fixa
                # Scores individuais (podem ser ajustados conforme necessário)
                "score_colley": None,
                "score_massey": None,
                "score_elo_final": None,
                "score_elo_mov": None,
                "score_trueskill": None,
                "score_pagerank": None,
                "score_bradley_terry": None,
                "score_pca": None,
                "score_sos": None,
                "score_consistency": None,
                "score_integrado": None
            }
            ranking_history_entries.append(entry)
        
        # Insere em lotes
        batch_size = 100
        for i in range(0, len(ranking_history_entries), batch_size):
            batch = ranking_history_entries[i:i + batch_size]
            supabase.table("ranking_history").insert(batch).execute()
        
        print(f"\n✅ Snapshot #{snapshot_id} criado!")
        print(f"   Times:    {snapshot['total_teams']}")
        print(f"   Partidas: {snapshot['total_matches']}")
        
        # Atualiza current_ranking nas teams
        for team in ranking:
            if team.get("team_id"):
                supabase.table("teams").update({
                    "current_ranking_position": team["position"],
                    "current_ranking_score": float(team["points"]),
                    "current_ranking_games": team["matches_played"],
                    "current_ranking_snapshot_id": snapshot_id,
                    "current_ranking_updated_at": datetime.now(timezone.utc).isoformat()
                }).eq("id", team["team_id"]).execute()
        
        # Salva arquivo local com dados completos
        full_snapshot = get_snapshot_with_ranking(snapshot_id)
        save_snapshot_file(full_snapshot)
        
    except Exception as e:
        print(f"\n❌ Falha: {e}")
    input("\nEnter para continuar.")


def export_snapshot_details() -> None:
    """Baixa um snapshot específico e salva localmente."""
    sid = input("\nID do snapshot para baixar (Enter cancela): ").strip()
    if not sid or not sid.isdigit():
        return
    try:
        snapshot = get_snapshot_with_ranking(int(sid))
        save_snapshot_file(snapshot)
    except Exception as e:
        print(f"\n❌ Falha: {e}")
    input("\nEnter para continuar.")


def show_snapshots_history() -> None:
    """Lista os snapshots disponíveis no banco."""
    try:
        result = (
            supabase.table("ranking_snapshots")
            .select("id, created_at, total_teams, total_matches, snapshot_metadata")
            .order("created_at", desc=True)
            .limit(20)
            .execute()
        )
        
        if not result.data:
            print("\n📊 Nenhum snapshot no histórico.")
        else:
            print(
                f"\n{'ID':>4} | {'Data/Hora':^20} | {'Times':>5} | {'Partidas':>8} | Arq"
            )
            print("-" * 60)
            for snap in result.data:
                dt = datetime.fromisoformat(
                    snap["created_at"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y %H:%M")
                file_flag = "📂" if (SAVE_DIR / f"{snap['id']}.json").exists() else "—"
                print(
                    f"{snap['id']:>4} | {dt:^20} | {snap['total_teams']:>5} | "
                    f"{snap['total_matches']:>8} | {file_flag}"
                )
    except Exception as e:
        print(f"\n❌ Erro ao listar: {e}")
    input("\nEnter para continuar.")


def delete_snapshot() -> None:
    """Exclui snapshot do banco + arquivo local."""
    # Lista snapshots
    result = (
        supabase.table("ranking_snapshots")
        .select("id, created_at, total_teams")
        .order("created_at", desc=True)
        .limit(30)
        .execute()
    )
    
    snaps = result.data
    if not snaps:
        print("\n📊 Nenhum snapshot para excluir.")
        input("\nEnter…")
        return
    if len(snaps) == 1:
        print("\n⚠️  Não é possível excluir o único snapshot existente.")
        input("\nEnter…")
        return

    print("\n🗑️  Snapshots disponíveis:")
    print(f"\n{'ID':>4} | {'Data':^20} | {'Times':>5}")
    print("-" * 40)
    for i, s in enumerate(snaps):
        dt = datetime.fromisoformat(
            s["created_at"].replace("Z", "+00:00")
        ).strftime("%d/%m/%y %H:%M")
        lock = " 🔒" if i == 0 else ""
        print(f"{s['id']:>4} | {dt:^20} | {s['total_teams']:>5}{lock}")

    sid = input("\nID para excluir (Enter cancela): ").strip()
    if not sid or not sid.isdigit():
        return
    sid_int = int(sid)
    
    if sid_int == snaps[0]["id"]:
        confirm = input(
            "⚠️ Excluir o snapshot mais recente afeta variações. Digite 'SIM': "
        )
        if confirm != "SIM":
            return

    # Exclui do banco (ranking_history será excluído em cascata)
    try:
        # Primeiro exclui da ranking_history
        supabase.table("ranking_history").delete().eq("snapshot_id", sid_int).execute()
        
        # Depois exclui o snapshot
        supabase.table("ranking_snapshots").delete().eq("id", sid_int).execute()
        print(f"\n✅ Excluído snapshot #{sid_int}")
        
        # Remove arquivo local
        local_file = SAVE_DIR / f"{sid}.json"
        if local_file.exists():
            local_file.unlink()
            print("   Arquivo local removido.")
    except Exception as e:
        print(f"\n❌ Erro ao excluir: {e}")
    input("\nEnter…")


def cleanup_old_snapshots() -> None:
    """Exclui em lote snapshots antigos, mantendo N mais recentes."""
    result = (
        supabase.table("ranking_snapshots")
        .select("id, created_at")
        .order("created_at", desc=True)
        .limit(100)
        .execute()
    )
    
    snaps = result.data
    if len(snaps) <= 5:
        print(f"\n📊 Apenas {len(snaps)} snapshot(s) – nada para limpar.")
        input("\nEnter…")
        return

    keep = input("Manter quantos snapshots mais recentes? (>=3): ").strip()
    try:
        keep_n = int(keep)
        if keep_n < 3 or keep_n >= len(snaps):
            raise ValueError
    except ValueError:
        print("Número inválido.")
        input("\nEnter…")
        return

    to_delete = snaps[keep_n:]
    print(f"\n⚠️  {len(to_delete)} snapshots serão excluídos PERMANENTEMENTE.")
    confirm = input("Digite 'LIMPAR' para prosseguir: ")
    if confirm != "LIMPAR":
        return

    ok = fail = 0
    for s in to_delete:
        try:
            # Exclui ranking_history primeiro
            supabase.table("ranking_history").delete().eq("snapshot_id", s["id"]).execute()
            # Depois o snapshot
            supabase.table("ranking_snapshots").delete().eq("id", s["id"]).execute()
            ok += 1
            (SAVE_DIR / f"{s['id']}.json").unlink(missing_ok=True)
        except:
            fail += 1

    print(f"\n✅ {ok} excluídos, ❌ {fail} falhas.")
    input("\nEnter.")


# ───────────────────────── Funções utilitárias ────────────────────────── #


def test_connection() -> bool:
    """Testa conexão com o Supabase."""
    print("\n🔌 Testando conexão…")
    try:
        # Tenta buscar um registro qualquer
        result = supabase.table("teams").select("slug").limit(1).execute()
        
        # Conta registros
        teams_count = supabase.table("teams").select("slug", count="exact").execute().count
        matches_count = supabase.table("matches").select("idPartida", count="exact").execute().count
        
        print(f"✅ Conexão OK – {teams_count} times, {matches_count} partidas")
        return True
    except Exception as e:
        print(f"❌ Falha na conexão: {e}")
        return False


def print_header() -> None:
    clear_console()
    print(
        "\n📂  Gerenciador de Snapshots – Valorant Universitário (Supabase)\n"
        "───────────────────────────────────────────────────────────────────"
    )


# ───────────────────────────── Loop principal ─────────────────────────── #


def main() -> None:
    if not test_connection():
        print("\n❌ Não foi possível conectar ao Supabase.")
        print("   Verifique SUPABASE_URL e SUPABASE_SERVICE_ROLE no .env")
        input("\nEnter para sair…")
        return

    while True:
        print_header()
        latest = check_latest_snapshot()

        print(
            """
📋 Opções:
1  – Capturar novo snapshot
2  – Ver histórico de snapshots
3  – Baixar / salvar detalhes de um snapshot
4  – Excluir snapshot
5  – Limpar snapshots antigos
6  – Testar conexão
7  – Configurações
8  – Sair
"""
        )
        choice = input("Escolha (1-8): ").strip()
        if choice == "1":
            if latest:
                created_at = datetime.fromisoformat(latest["created_at"].replace("Z", "+00:00"))
                hours_ago = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
                if hours_ago < 24:
                    confirm = input(
                        f"Snapshot recente ({int(hours_ago)}h atrás). Criar mesmo assim? (s/n): "
                    )
                    if confirm.lower() != "s":
                        continue
            capture_new_snapshot()
        elif choice == "2":
            show_snapshots_history()
        elif choice == "3":
            export_snapshot_details()
        elif choice == "4":
            delete_snapshot()
        elif choice == "5":
            cleanup_old_snapshots()
        elif choice == "6":
            test_connection()
            input("\nEnter.")
        elif choice == "7":
            print("\n⚙️  Configurações:")
            print(f"   SUPABASE_URL: {SUPABASE_URL}")
            print(f"   SERVICE_ROLE: {'*' * 20}...{SUPABASE_SERVICE_ROLE[-10:]}")
            print(f"   SAVE_DIR:     {SAVE_DIR}")
            input("\nEnter.")
        elif choice == "8":
            print("\n👋 Até logo!")
            break
        else:
            print("Opção inválida.")
            input("\nEnter…")


# ─────────────────────────── Execução direta ──────────────────────────── #

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        if platform.system() == "Windows":
            input("\nEnter para sair…")
        sys.exit(1)