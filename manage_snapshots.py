import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# ─────────────────────────── Config e diretórios ─────────────────────────── #

load_dotenv()
API_URL: str = os.getenv("API_URL", "https://apiuni.onrender.com").rstrip("/")
ADMIN_KEY: str = os.getenv("ADMIN_KEY", "valorant2024admin")
RANKING_REFRESH_KEY: str = os.getenv("RANKING_REFRESH_KEY", "valorant2024ranking")

SAVE_DIR = Path(__file__).with_name("snapshots_data")
SAVE_DIR.mkdir(exist_ok=True)

DEFAULT_LIST_PARAMS = {"include_full_data": "true"}  # ranking completo

TIMEOUT_SHORT = 8
TIMEOUT_MED = 20
TIMEOUT_LONG = 120

# ─────────────────────────── HTTP Session com Retry ──────────────────────── #

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "SnapshotManager/2.0"})
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST", "DELETE"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

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


def _extract_snapshot_id(snapshot_payload: Dict[str, Any]) -> int:
    if "id" in snapshot_payload:
        return int(snapshot_payload["id"])
    # fallback: alguns endpoints retornam {'snapshot': {...}}
    if "snapshot" in snapshot_payload and isinstance(snapshot_payload["snapshot"], dict):
        sid = snapshot_payload["snapshot"].get("id")
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


# ───────────────────────────── API Client --------------------------------- #

def load_snapshots(limit: int = 20) -> List[Dict[str, Any]]:
    """Faz GET /ranking/snapshots e devolve lista (campo data)."""
    resp = SESSION.get(
        f"{API_URL}/ranking/snapshots",
        params={"limit": limit, **DEFAULT_LIST_PARAMS},
        timeout=TIMEOUT_SHORT,
    )
    resp.raise_for_status()
    body = resp.json()
    return body.get("data") or body.get("snapshots") or []


def fetch_snapshot_details(snap_id: int) -> Dict[str, Any]:
    """Faz GET /ranking/snapshots/{id}/details."""
    resp = SESSION.get(
        f"{API_URL}/ranking/snapshots/{snap_id}/details",
        timeout=TIMEOUT_LONG,
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
        print(f"   Times:       {snap.get('total_teams')}")
        print(f"   Partidas:    {snap.get('total_matches')}")
        return snap
    except Exception as e:
        print(f"\n❌ Falha ao consultar snapshot mais recente: {e}")
        return None


def capture_new_snapshot() -> None:
    """POST /ranking/snapshot (admin). Salva automaticamente o JSON completo."""
    print("\n🔄 Capturando novo snapshot…")
    try:
        resp = SESSION.post(
            f"{API_URL}/ranking/snapshot",
            params={"admin_key": ADMIN_KEY},
            timeout=TIMEOUT_LONG,
        )
        resp.raise_for_status()
        meta = resp.json()
        snap_id = meta.get("snapshot_id") or meta.get("id")
        if not snap_id:
            print(f"⚠️ Resposta sem snapshot_id: {meta}")
            return
        print(f"\n✅ Snapshot #{snap_id} criado!")
        full = fetch_snapshot_details(int(snap_id))
        save_snapshot_file(full)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            print("\n❌ Chave de administrador inválida")
        else:
            print(f"\n❌ HTTP {e.response.status_code if e.response else ''}: {e}")
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
                f"\n{'ID':>4} | {'Data':^20} | {'Times':>5} | {'Partidas':>9}\n"
                + ("-" * 48)
            )
            for s in snaps:
                print(
                    f"{s['id']:>4} | {human_diff(s['created_at']):^20} | {s.get('total_teams', 0):>5} | {s.get('total_matches', 0):>9}"
                )
    except Exception as e:
        print(f"\n❌ Falha: {e}")
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
    for s in snaps:
        print(f"{s['id']:>4} | {human_diff(s['created_at']):^20} | {s.get('total_teams', 0):>5}")

    sid = input("\nID do snapshot a excluir (Enter cancela): ").strip()
    if not sid.isdigit():
        return
    sid = int(sid)

    if sid == snaps[0]["id"]:
        confirm = input("\n⚠️ Você está prestes a excluir o snapshot MAIS RECENTE. Digite 'SIM' para confirmar: ").strip()
        if confirm != "SIM":
            return

    # chamada DELETE
    try:
        resp = SESSION.delete(
            f"{API_URL}/ranking/snapshots/{sid}",
            params={"admin_key": ADMIN_KEY},
            timeout=TIMEOUT_MED,
        )
        resp.raise_for_status()
        print(f"\n✅ Excluído #{sid}")
        local_file = SAVE_DIR / f"{sid}.json"
        if local_file.exists():
            local_file.unlink()
            print("   Arquivo local removido.")
        # força refresh cache
        try:
            r = SESSION.post(
                f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}, timeout=TIMEOUT_SHORT
            )
            r.raise_for_status()
        except Exception:
            pass
    except requests.HTTPError as e:
        print(f"\n❌ HTTP {e.response.status_code if e.response else ''}: {e}")
    except Exception as e:
        print(f"\n❌ Falha: {e}")
    input("\nEnter…")


def cleanup_old_snapshots() -> None:
    """Exclui em lote snapshots antigos, mantendo N mais recentes."""
    snaps = load_snapshots(limit=100)
    if len(snaps) <= 5:
        print(f"\n📊 Apenas {len(snaps)} snapshot(s) – nada para limpar.")
        input("\nEnter…")
        return

    keep_n = input("\nQuantos snapshots manter? [padrão 5]: ").strip()
    keep = int(keep_n) if keep_n.isdigit() else 5
    to_delete = snaps[keep:]
    if not to_delete:
        print("\nNada para limpar.")
        input("\nEnter…")
        return

    print(f"\n⚠️  {len(to_delete)} snapshots serão excluídos PERMANENTEMENTE da API.")
    confirm = input("Digite 'LIMPAR' para prosseguir: ")
    if confirm != "LIMPAR":
        return

    ok = fail = 0
    for s in to_delete:
        try:
            resp = SESSION.delete(
                f"{API_URL}/ranking/snapshots/{s['id']}",
                params={"admin_key": ADMIN_KEY},
                timeout=TIMEOUT_MED,
            )
            resp.raise_for_status()
            ok += 1
            (SAVE_DIR / f"{s['id']}.json").unlink(missing_ok=True)
        except Exception:
            fail += 1

    print(f"\n✅ {ok} excluídos, ❌ {fail} falhas.")
    if ok:
        try:
            r = SESSION.post(
                f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}, timeout=TIMEOUT_SHORT
            )
            r.raise_for_status()
        except Exception:
            pass
    input("\nEnter.")


# ───────────────────────── Funções utilitárias ────────────────────────── #


def test_connection() -> bool:
    """Ping na API + /info para checar estado geral."""
    print("\n🔌 Testando conexão…")
    try:
        resp = SESSION.get(f"{API_URL}/health", timeout=TIMEOUT_SHORT)
        resp.raise_for_status()
        info_resp = SESSION.get(f"{API_URL}/info", timeout=TIMEOUT_SHORT)
        info_resp.raise_for_status()
        info = info_resp.json()
        print(
            f"✅ API {info['api']['version']} – ranking "
            f"{'ON' if info['features']['ranking_available'] else 'OFF'}"
        )
        if info.get('last_snapshot'):
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
    resp = SESSION.post(
        f"{API_URL}/ranking/refresh", params={"secret_key": RANKING_REFRESH_KEY}, timeout=TIMEOUT_SHORT
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


# ───────────────────────── Extras: Preview/Export ─────────────────────── #

def preview_ranking(limit: int = 50) -> None:
    """Consulta /ranking/preview e salva JSON local (não publica)."""
    try:
        resp = SESSION.get(f"{API_URL}/ranking/preview", params={"limit": limit}, timeout=TIMEOUT_LONG)
        resp.raise_for_status()
        payload = resp.json()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = SAVE_DIR / f"preview_{ts}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\n🧪 Preview salvo em {path}")
        top = payload.get("ranking", [])[:5]
        if top:
            print("\nTop 5 do preview:")
            for i, r in enumerate(top, 1):
                team = r.get('team') or r.get('name') or '???'
                tag = r.get('tag') or ''
                nota = r.get('nota_final') or 0
                print(f"{i:>2}. {team} ({tag}) – {nota:.2f}")
    except Exception as e:
        print(f"\n❌ Falha no preview: {e}")
    input("\nEnter.")


def export_history_csv(filename: str = "snapshots_index.csv", limit: int = 100) -> None:
    """Gera CSV (id, created_at, total_teams, total_matches) do histórico."""
    try:
        snaps = load_snapshots(limit=limit)
        if not snaps:
            print("\nℹ️ Sem snapshots para exportar.")
            return
        rows = [
            {
                "id": s["id"],
                "created_at": s["created_at"],
                "total_teams": s.get("total_teams"),
                "total_matches": s.get("total_matches"),
            }
            for s in snaps
        ]
        path = SAVE_DIR / filename
        import csv
        with path.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"📄 CSV gerado: {path}")
    except Exception as e:
        print(f"\n❌ Falha ao exportar CSV: {e}")
    input("\nEnter.")


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
7  – Preview do ranking (não publica)
8  – Exportar histórico (CSV)
9  – Testar conexão
0  – Sair
"""
        )
        choice = input("Escolha (0-9): ").strip()
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
            preview_ranking()
        elif choice == "8":
            export_history_csv()
        elif choice == "9":
            test_connection(); input("\nEnter.")
        elif choice == "0":
            break
        else:
            continue


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