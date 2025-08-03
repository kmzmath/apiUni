#!/usr/bin/env python
"""
Script para verificar e corrigir snapshots que foram importados 
com mapeamento incorreto de times (ex: maua_pipao vs maua_rbty)
"""

import os
from datetime import datetime
from typing import Dict, List, Any

from dotenv import load_dotenv
from supabase import create_client, Client

# Carrega variáveis de ambiente
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")

# Inicializa cliente Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
supabase.postgrest.auth(SUPABASE_SERVICE_ROLE)


def analyze_tag_conflicts():
    """Analisa conflitos de tags entre times"""
    print("🔍 Analisando conflitos de tags...\n")
    
    # Busca todos os times
    teams = supabase.table("teams").select("id, slug, name, tag").execute().data
    
    # Agrupa por tag
    teams_by_tag = {}
    for team in teams:
        if team.get("tag"):
            tag = team["tag"]
            if tag not in teams_by_tag:
                teams_by_tag[tag] = []
            teams_by_tag[tag].append(team)
    
    # Identifica tags com múltiplos times
    conflicts = {tag: teams for tag, teams in teams_by_tag.items() if len(teams) > 1}
    
    if conflicts:
        print("⚠️  Tags compartilhadas por múltiplos times:")
        for tag, conflicting_teams in conflicts.items():
            print(f"\n   Tag '{tag}':")
            for team in conflicting_teams:
                print(f"     - {team['name']} (slug: {team['slug']}, id: {team['id']})")
    else:
        print("✅ Nenhum conflito de tag encontrado!")
    
    return conflicts


def check_maua_teams_in_snapshots():
    """Verifica especificamente os times Mauá nos snapshots"""
    print("\n\n🔍 Verificando times Mauá nos snapshots...")
    
    # Busca os times Mauá
    maua_teams = (
        supabase.table("teams")
        .select("id, slug, name, tag")
        .in_("slug", ["maua_pipao", "maua_rbty"])
        .execute()
    ).data
    
    if len(maua_teams) != 2:
        print("❌ Erro: Não encontrei exatamente 2 times Mauá")
        return
    
    # Mapeia slug para id
    maua_map = {team["slug"]: team for team in maua_teams}
    pipao_id = maua_map["maua_pipao"]["id"]
    rbty_id = maua_map["maua_rbty"]["id"]
    
    print(f"\n   Mauá Esports A (maua_pipao): ID {pipao_id}")
    print(f"   Mauá Esports B (maua_rbty): ID {rbty_id}")
    
    # Busca todos os snapshots
    snapshots = (
        supabase.table("ranking_snapshots")
        .select("id, created_at, total_teams")
        .order("created_at")
        .execute()
    ).data
    
    print(f"\n📊 Total de snapshots: {len(snapshots)}")
    
    # Para cada snapshot, verifica quais times Mauá estão presentes
    snapshots_with_issues = []
    
    for snapshot in snapshots:
        ranking = (
            supabase.table("ranking_history")
            .select("team_id, position, nota_final")
            .eq("snapshot_id", snapshot["id"])
            .in_("team_id", [pipao_id, rbty_id])
            .execute()
        ).data
        
        # Analisa o resultado
        teams_found = {entry["team_id"] for entry in ranking}
        
        if pipao_id in teams_found and rbty_id in teams_found:
            status = "✅ Ambos times presentes"
        elif pipao_id in teams_found:
            status = "⚠️  Apenas maua_pipao"
        elif rbty_id in teams_found:
            status = "⚠️  Apenas maua_rbty"
        else:
            status = "❌ Nenhum time Mauá"
        
        if "⚠️" in status:
            snapshots_with_issues.append({
                "snapshot_id": snapshot["id"],
                "created_at": snapshot["created_at"],
                "status": status,
                "teams_found": teams_found
            })
        
        print(f"\n   Snapshot {snapshot['created_at'][:10]}: {status}")
        for entry in ranking:
            team_name = "maua_pipao" if entry["team_id"] == pipao_id else "maua_rbty"
            print(f"      - {team_name}: posição {entry['position']}, nota {entry['nota_final']:.2f}")
    
    return snapshots_with_issues


def suggest_fixes(issues: List[Dict[str, Any]]):
    """Sugere correções para os problemas encontrados"""
    if not issues:
        print("\n✅ Nenhum problema encontrado!")
        return
    
    print(f"\n\n⚠️  Encontrados {len(issues)} snapshots com possíveis problemas")
    print("\nPossíveis causas:")
    print("1. Os snapshots antigos usavam apenas a tag 'MAUA' sem especificar o time")
    print("2. O mapeamento original sobrescreveu maua_pipao com maua_rbty")
    print("3. Um dos times pode não ter participado de alguns períodos")
    
    print("\n📝 Recomendações:")
    print("1. Verifique os arquivos JSON originais dos snapshots com problemas")
    print("2. Procure por referências específicas aos jogadores para identificar o time correto")
    print("3. Se necessário, reimporte os snapshots usando o script corrigido")
    print("4. Para snapshots mais recentes, verifique se ambos os times deveriam estar presentes")


def main():
    """Função principal"""
    print("🔧 Verificador de Snapshots - Times Mauá\n")
    
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE no .env")
        return
    
    # 1. Analisa conflitos de tags
    conflicts = analyze_tag_conflicts()
    
    # 2. Verifica especificamente os times Mauá
    issues = check_maua_teams_in_snapshots()
    
    # 3. Sugere correções
    suggest_fixes(issues)
    
    print("\n" + "=" * 60)
    print("✅ Análise concluída!")
    
    if issues:
        print(f"\n⚠️  Ação recomendada: Re-importe os snapshots problemáticos")
        print("   usando o script import_old_snapshots_to_supabase.py corrigido")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Verificação interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {str(e)}")
        import traceback
        traceback.print_exc()