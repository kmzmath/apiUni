import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# ─────────────────────────── Config e diretórios ─────────────────────────── #

load_dotenv()
API_URL: str = os.getenv("API_URL", "https://apiuni.onrender.com/")
ADMIN_KEY: str = os.getenv("ADMIN_KEY", "valorant2024admin")
RANKING_REFRESH_KEY: str = os.getenv("RANKING_REFRESH_KEY", "valorant2024ranking")

SAVE_DIR = Path(__file__).with_name("snapshots_data")
SAVE_DIR.mkdir(exist_ok=True)

DEFAULT_LIST_PARAMS = {"include_full_data": "true"}  # ranking completo

# ─────────────────────────── Helpers genéricos ─────────────────────────── #


def clear_console() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def human_diff(created_at_iso: str) -> str:
    dt = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    diff = datetime.now(timezone.utc) - dt
    hours = diff.total_seconds() / 3600
    if hours < 1:
        return f"{int(diff.total_seconds() / 60)} min atrás"
    if hours < 24:
        return f"{int(hours)} h atrás"
    return f"{int(hours // 24)} d atrás"


def _extract_snapshot_id(payload: Dict[str, Any]) -> int:
    """Aceita payloads de /ranking/snapshots ou /details e devolve o id."""
    if isinstance(payload.get("id"), int):
        return payload["id"]
    if isinstance(payload.get("snapshot"), dict):
        sid = payload["snapshot"].get("id")
        if isinstance(sid, int):
            return sid
    raise KeyError("Campo 'id' não encontrado no payload")


def save_snapshot_file(snapshot_payload: Dict[str, Any]) -> None:
    """Salva o snapshot (payload completo) em snapshots_data/{id}.json."""
    try:
        sid = _extract_snapshot_id(snapshot_payload)
    except KeyError:
        print("❌ Payload sem campo 'id' – não pôde salvar.")
        return

    path = SAVE_DIR / f"{sid}.json"
    with path.open("w", encoding="utf-8") as fp:
        json.dump(snapshot_payload, fp, indent=2, ensure_ascii=False)

    rel = path.relative_to(Path.cwd())
    print(f"💾  Arquivo salvo: {rel}")


def load_snapshots(limit: int = 20) -> List[Dict[str, Any]]:
    """Faz GET /ranking/snapshots e devolve lista (campo data)."""
    resp = requests.get(
        f"{API_URL}/ranking/snapshots",
        params={"limit": limit, **DEFAULT_LIST_PARAMS},
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    return body.get("data") or body.get("snapshots") or []


def fetch_snapshot_details(snap_id: int) -> Dict[str, Any]:
    """Faz GET /ranking/snapshots/{id}/details."""
    resp = requests.get(
        f"{API_URL}/ranking/snapshots/{snap_id}/details",
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


# ─────────────────────── Operações principais ────────────────────────── #


def check_latest_snapshot() -> Optional[Dict[str, Any]]:
    """Mostra informações do snapshot mais recente (se existir)."""
    try:
        latest = load_snapshots(limit=1)
        if not latest:
            print("\n⚠️  Nenhum snapshot encontrado ainda.")
            return None

        snap = latest[0]
        print("\n📊 Último Snapshot:")
        print(f"   ID:          {snap['id']}")
        print(f"   Capturado:   {human_diff(snap['created_at'])}")
        print(f"   Times:       {snap['total_teams']}")
        print(f"   Partidas:    {snap['total_matches']}")
        return snap
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        return None


def capture_new_snapshot() -> None:
    """POST /ranking/snapshot (admin). Salva automaticamente o JSON completo."""
    print("\n🔄 Capturando novo snapshot…")
    try:
        resp = requests.post(
            f"{API_URL}/ranking/snapshot",
            params={"admin_key": ADMIN_KEY},
            timeout=120,
        )
        if resp.status_code == 200:
            meta = resp.json()
            print(f"\n✅ Snapshot #{meta['snapshot_id']} criado!")
            full = fetch_snapshot_details(meta["snapshot_id"])
            save_snapshot_file(full)
        elif resp.status_code == 403:
            print("\n❌ Chave de administrador inválida")
        else:
            print(f"\n❌ Erro HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"\n❌ Falha: {e}")
    input("\nEnter para continuar.")


def export_snapshot_details() -> None:
    """Baixa um snapshot específico e salva localmente."""
    sid = input("\nID do snapshot para baixar (Enter cancela): ").strip()
    if not sid.isdigit():
        return
    try:
        data = fetch_snapshot_details(int(sid))
        save_snapshot_file(data)
    except Exception as e:
        print(f"\n❌ Falha: {e}")
    input("\nEnter para continuar.")


def show_snapshots_history() -> None:
    """Lista os snapshots disponíveis na API (máx. 20)."""
    try:
        snaps = load_snapshots(limit=20)
        if not snaps:
            print("\n📊 Nenhum snapshot no histórico.")
        else:
            print(
                f"\n{'ID':>4} | {'Data/Hora':^20} | {'Times':>5} | {'Partidas':>8} | Arq"
            )
            print("-" * 60)
            for snap in snaps:
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
    """Exclui snapshot da API + arquivo local, com confirmações."""
    snaps = load_snapshots(limit=30)
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
    if not sid:
        return
    if sid == str(snaps[0]["id"]):
        confirm = input(
            "⚠️ Excluir o snapshot mais recente afeta variações. Digite 'SIM': "
        )
        if confirm != "SIM":
            return

    # chamada DELETE
    resp = requests.delete(
        f"{API_URL}/ranking/snapshots/{sid}",
        params={"admin_key": ADMIN_KEY},
        timeout=30,
    )
    if resp.status_code == 200:
        print(f"\n✅ Excluído #{sid}")
        local_file = SAVE_DIR / f"{sid}.json"
        if local_file.exists():
            local_file.unlink()
            print("   Arquivo local removido.")
        # força refresh cache
        requests.post(
            f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}
        )
    else:
        print(f"\n❌ HTTP {resp.status_code}: {resp.text}")
    input("\nEnter…")


def cleanup_old_snapshots() -> None:
    """Exclui em lote snapshots antigos, mantendo N mais recentes."""
    snaps = load_snapshots(limit=100)
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
    print(f"\n⚠️  {len(to_delete)} snapshots serão excluídos PERMANENTEMENTE da API.")
    confirm = input("Digite 'LIMPAR' para prosseguir: ")
    if confirm != "LIMPAR":
        return

    ok = fail = 0
    for s in to_delete:
        resp = requests.delete(
            f"{API_URL}/ranking/snapshots/{s['id']}",
            params={"admin_key": ADMIN_KEY},
            timeout=30,
        )
        if resp.status_code == 200:
            ok += 1
            (SAVE_DIR / f"{s['id']}.json").unlink(missing_ok=True)
        else:
            fail += 1

    print(f"\n✅ {ok} excluídos, ❌ {fail} falhas.")
    if ok:
        requests.post(
            f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}
        )
    input("\nEnter.")


# ───────────────────────── Funções utilitárias ────────────────────────── #


def test_connection() -> bool:
    """Ping na API + /info para checar estado geral."""
    print("\n🔌 Testando conexão…")
    try:
        resp = requests.get(f"{API_URL}/health", timeout=5)
        if resp.status_code != 200:
            print(f"❌ HTTP {resp.status_code}")
            return False
        info = requests.get(f"{API_URL}/info", timeout=5).json()
        print(
            f"✅ API {info['api']['version']} – ranking "
            f"{'ON' if info['features']['ranking_available'] else 'OFF'}"
        )
        if info.get("last_snapshot"):
            print(
                f"   Último snapshot: {info['last_snapshot']['time_since']['human_readable']}"
            )
        return True
    except Exception as e:
        print(f"❌ Falha: {e}")
        return False


def force_ranking_refresh() -> None:
    """Chama /ranking/refresh (endpoint público) para limpar cache."""
    print("\n🔄 Forçando recálculo…")
    resp = requests.post(
        f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}
    )
    if resp.status_code == 200:
        print("✅ Cache limpo / recálculo disparado.")
    else:
        print(f"❌ HTTP {resp.status_code}: {resp.text}")


def print_header() -> None:
    clear_console()
    print(
        "\n📂  Gerenciador de Snapshots – Valorant Universitário\n"
        "─────────────────────────────────────────────────────"
    )


# ───────────────────────────── Loop principal ─────────────────────────── #


def main() -> None:
    if not test_connection():
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
6  – Forçar recálculo do ranking
7  – Testar conexão
8  – Configurações
9  – Sair
"""
        )
        choice = input("Escolha (1-9): ").strip()
        if choice == "1":
            if latest and human_diff(latest["created_at"]).endswith("h atrás"):
                confirm = input(
                    "Snapshot recente (<24 h). Criar mesmo assim? (s/n): "
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
            force_ranking_refresh()
            input("\nEnter.")
        elif choice == "7":
            test_connection()
            input("\nEnter.")
        elif choice == "8":
            print("\n⚙️  Configurações:")
            print(f"   API_URL:   {API_URL}")
            print(f"   ADMIN_KEY: {'*' * (len(ADMIN_KEY) - 4)}{ADMIN_KEY[-4:]}")
            print(f"   SAVE_DIR:  {SAVE_DIR}")
            input("\nEnter.")
        elif choice == "9":
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