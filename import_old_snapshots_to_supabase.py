#!/usr/bin/env python
"""
Script para importar snapshots antigos (JSON) para o Supabase
Mapeia a estrutura antiga para as tabelas ranking_snapshots e ranking_history
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from dotenv import load_dotenv
from supabase import create_client, Client

# Carrega variáveis de ambiente
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")

# Inicializa cliente Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
supabase.postgrest.auth(SUPABASE_SERVICE_ROLE)

# Diretório com os JSONs antigos
SNAPSHOTS_DIR = Path(".")  # Diretório atual, ajuste conforme necessário


def get_team_mapping() -> Dict[str, int]:
    """
    Cria um mapeamento de team_slug/tag/nome para team_id atual.
    """
    teams = supabase.table("teams").select("id, slug, name, tag").execute().data
    
    mapping = {}
    for team in teams:
        # Mapeia por slug
        if team.get("slug"):
            mapping[team["slug"]] = team["id"]
        # Mapeia por tag
        if team.get("tag"):
            mapping[team["tag"]] = team["id"]
        # Mapeia por nome
        if team.get("name"):
            mapping[team["name"]] = team["id"]
    
    return mapping


def find_team_id(team_data: Dict[str, Any], team_mapping: Dict[str, int]) -> Optional[int]:
    """
    Tenta encontrar o ID do time no banco atual usando diferentes campos.
    """
    # Tenta primeiro pelo team_id do JSON (se existir e for válido)
    if "team_id" in team_data:
        team_id = team_data["team_id"]
        # Verifica se este ID ainda existe
        result = supabase.table("teams").select("id").eq("id", team_id).execute()
        if result.data:
            return team_id
    
    # Tenta por slug
    if "team_slug" in team_data and team_data["team_slug"] in team_mapping:
        return team_mapping[team_data["team_slug"]]
    
    # Tenta por tag
    if "tag" in team_data and team_data["tag"] in team_mapping:
        return team_mapping[team_data["tag"]]
    
    # Tenta por nome
    if "team" in team_data and team_data["team"] in team_mapping:
        return team_mapping[team_data["team"]]
    
    return None


def import_snapshot_file(file_path: Path, team_mapping: Dict[str, int]) -> bool:
    """
    Importa um arquivo JSON de snapshot para o Supabase.
    """
    print(f"\n📄 Processando: {file_path.name}")
    
    try:
        # Lê o arquivo JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extrai informações do snapshot
        snapshot_info = data.get("snapshot", {})
        ranking_data = data.get("ranking", [])
        
        # Verifica se já foi importado (baseado em created_at e total_teams)
        created_at = snapshot_info.get("created_at")
        if created_at:
            existing = (
                supabase.table("ranking_snapshots")
                .select("id")
                .eq("created_at", created_at)
                .eq("total_teams", snapshot_info.get("total_teams"))
                .execute()
            )
            if existing.data:
                print(f"   ⏭️  Já importado (ID: {existing.data[0]['id']})")
                return False
        
        # Prepara metadados do snapshot
        metadata = snapshot_info.get("metadata", {})
        metadata["imported_from"] = "old_json_files"
        metadata["original_snapshot_id"] = snapshot_info.get("id")
        metadata["import_date"] = datetime.now().isoformat()
        metadata["algorithms_used"] = metadata.get("algorithms_used", [])
        
        # Cria o snapshot
        snapshot_data = {
            "total_teams": snapshot_info.get("total_teams", len(ranking_data)),
            "total_matches": snapshot_info.get("total_matches", 0),
            "snapshot_metadata": metadata
        }
        
        # Se tiver created_at original, usa ele
        if created_at:
            snapshot_data["created_at"] = created_at
        
        snapshot_result = supabase.table("ranking_snapshots").insert(snapshot_data).execute()
        
        if not snapshot_result.data:
            print("   ❌ Erro ao criar snapshot")
            return False
        
        snapshot_id = snapshot_result.data[0]["id"]
        print(f"   ✅ Snapshot criado (ID: {snapshot_id})")
        
        # Importa dados do ranking
        ranking_entries = []
        skipped_teams = []
        processed_teams = set()  # Para evitar duplicatas
        
        for team_data in ranking_data:
            # Encontra o team_id atual
            team_id = find_team_id(team_data, team_mapping)
            
            if not team_id:
                team_name = team_data.get("team", team_data.get("team_slug", "Unknown"))
                skipped_teams.append(team_name)
                continue
            
            # Evita duplicatas
            if team_id in processed_teams:
                continue
            processed_teams.add(team_id)
            
            # Prepara entrada do ranking_history
            entry = {
                "snapshot_id": snapshot_id,
                "team_id": team_id,
                "position": team_data.get("posicao", team_data.get("position", 0)),
                "games_count": team_data.get("games_count", 0),
                "nota_final": float(team_data.get("nota_final", 0.0)),
                "ci_lower": float(team_data.get("ci_lower", 0.0)),
                "ci_upper": float(team_data.get("ci_upper", 100.0)),
                "incerteza": float(team_data.get("incerteza", 0.0)),
            }
            
            # Adiciona scores individuais se disponíveis
            scores = team_data.get("scores", {})
            
            # Mapeia os campos de score
            score_mapping = {
                "colley": "score_colley",
                "massey": "score_massey",
                "elo": "score_elo_final",
                "elo_mov": "score_elo_mov",
                "trueskill": "score_trueskill",
                "pagerank": "score_pagerank",
                "bradley_terry": "score_bradley_terry",
                "pca": "score_pca",
                "sos": "score_sos",
                "consistency": "score_consistency",
                "integrado": "score_integrado"
            }
            
            for old_field, new_field in score_mapping.items():
                if old_field in scores and scores[old_field] is not None:
                    entry[new_field] = float(scores[old_field])
            
            ranking_entries.append(entry)
        
        # Insere em lotes
        if ranking_entries:
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(ranking_entries), batch_size):
                batch = ranking_entries[i:i + batch_size]
                try:
                    supabase.table("ranking_history").insert(batch).execute()
                    total_inserted += len(batch)
                except Exception as e:
                    print(f"   ⚠️  Erro ao inserir lote {i//batch_size + 1}: {str(e)}")
            
            print(f"   ✅ {total_inserted} times importados")
        
        if skipped_teams:
            print(f"   ⚠️  {len(skipped_teams)} times não encontrados:")
            for team in skipped_teams[:5]:  # Mostra apenas os primeiros 5
                print(f"      - {team}")
            if len(skipped_teams) > 5:
                print(f"      ... e mais {len(skipped_teams) - 5} times")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def update_current_ranking_from_latest_snapshot():
    """
    Atualiza os campos current_ranking_* nas teams baseado no último snapshot.
    """
    print("\n🔄 Atualizando ranking atual dos times...")
    
    # Busca o último snapshot
    latest = (
        supabase.table("ranking_snapshots")
        .select("id")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    
    if not latest.data:
        print("   ❌ Nenhum snapshot encontrado")
        return
    
    snapshot_id = latest.data[0]["id"]
    
    # Busca o ranking deste snapshot
    ranking = (
        supabase.table("ranking_history")
        .select("team_id, position, nota_final, games_count")
        .eq("snapshot_id", snapshot_id)
        .execute()
    )
    
    if not ranking.data:
        print("   ❌ Nenhum ranking encontrado para o snapshot")
        return
    
    # Atualiza cada time
    updated = 0
    for entry in ranking.data:
        try:
            supabase.table("teams").update({
                "current_ranking_position": entry["position"],
                "current_ranking_score": float(entry["nota_final"]),
                "current_ranking_games": entry["games_count"],
                "current_ranking_snapshot_id": snapshot_id,
                "current_ranking_updated_at": datetime.now().isoformat()
            }).eq("id", entry["team_id"]).execute()
            updated += 1
        except Exception as e:
            print(f"   ⚠️  Erro ao atualizar time {entry['team_id']}: {str(e)}")
    
    print(f"   ✅ {updated} times atualizados")


def main():
    """Função principal."""
    print("🔄 Importador de Snapshots Antigos para Supabase\n")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE no .env")
        return
    
    # Cria mapeamento de times
    print("📊 Carregando mapeamento de times...")
    team_mapping = get_team_mapping()
    print(f"   ✅ {len(team_mapping)} mapeamentos criados")
    
    # Lista arquivos JSON
    json_files = list(SNAPSHOTS_DIR.glob("*.json"))
    if not json_files:
        print(f"\n❌ Nenhum arquivo JSON encontrado em {SNAPSHOTS_DIR}")
        return
    
    print(f"\n📁 Encontrados {len(json_files)} arquivos JSON")
    
    # Ordena por nome (assumindo que o nome tem alguma ordem cronológica)
    json_files.sort()
    
    # Confirma importação
    print("\nArquivos a importar:")
    for f in json_files:
        print(f"  - {f.name}")
    
    confirm = input("\nDeseja continuar com a importação? (s/n): ")
    if confirm.lower() != 's':
        print("Importação cancelada.")
        return
    
    imported = 0
    skipped = 0
    errors = 0
    
    for file_path in json_files:
        try:
            result = import_snapshot_file(file_path, team_mapping)
            if result:
                imported += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            print(f"❌ Erro ao processar {file_path.name}: {str(e)}")
    
    print("\n" + "=" * 50)
    print(f"✅ Importados: {imported}")
    print(f"⏭️  Já existentes: {skipped}")
    print(f"❌ Erros: {errors}")
    print("=" * 50)
    
    # Atualiza current_ranking nos times
    if imported > 0:
        update_current_ranking_from_latest_snapshot()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Importação interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {str(e)}")
        import traceback
        traceback.print_exc()