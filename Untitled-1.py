#!/usr/bin/env python3
"""
EXECUTE ESTE ARQUIVO PARA CORRIGIR TODOS OS PROBLEMAS!
python fix_now.py
"""

print("🔧 Corrigindo main.py...")

# Lê o arquivo em UTF‑8 (removendo BOM se houver)
with open('main.py', 'r', encoding='utf-8-sig') as f:
    content = f.read()

# Backup (também em UTF‑8)
with open('main.py.backup', 'w', encoding='utf-8') as f:
    f.write(content)
print("✅ Backup criado: main.py.backup")

# 1. Adiciona AsyncSession se não existir
if 'from sqlalchemy.ext.asyncio import AsyncSession' not in content:
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'from database import get_db' in line:
            lines.insert(i+1, 'from sqlalchemy.ext.asyncio import AsyncSession')
            content = '\n'.join(lines)
            print("✅ AsyncSession importado")
            break
    else:
        content = 'from sqlalchemy.ext.asyncio import AsyncSession\n' + content
        print("✅ AsyncSession importado no início")

# 2. Lista COMPLETA de funções que precisam de async
all_functions = [
    # Endpoints principais
    'list_teams', 'search_teams', 'get_team', 'get_team_by_slug',
    'get_team_matches', 'get_team_stats', 'get_team_players',
    'get_team_ranking_history_old', 'get_team_tournaments',
    'get_team_complete_info', 'get_team_map_statistics',
    'get_team_map_comparison', 'get_team_social_media',
    'update_team_social_media', 'get_all_teams_players',
    'search_players', 'list_tournaments', 'get_tournament',
    'get_tournament_matches', 'list_matches', 'get_match',
    'get_maps_stats', 'get_general_stats', 'get_ranking',
    'list_snapshots', 'debug_ranking', 'preview_ranking',
    'get_ranking_stats', 'refresh_ranking_cache',
    'get_team_ranking_history', 'create_ranking_snapshot',
    'compare_ranking_snapshots', 'delete_ranking_snapshot',
    'get_snapshots_statistics', 'get_ranking_evolution',
    'get_fast_ranking', 'get_team_ranking_evolution',
    'get_ranking_movers', 'get_snapshot_details',
    'debug_team_data', 'get_api_info', 'list_estados',
    'get_estado', 'get_estado_teams', 'list_regioes',
    'get_estados_stats', 'get_team_with_estado',
    'calculate_ranking', 'root', 'health_check',
    '_calculate_map_rating', 'test_db'
]

# 3. Corrige cada função para async
fixes = 0
for func in all_functions:
    # padrão exato com quebra de linha antes
    old = f'\ndef {func}('
    new = f'\nasync def {func}('
    if old in content:
        content = content.replace(old, new)
        fixes += 1
        print(f"✅ {func} -> async (padrão 1)")
    # alternativa sem quebra
    old2 = f'def {func}('
    if old2 in content and f'async def {func}(' not in content:
        content = content.replace(old2, f'async def {func}(')
        fixes += 1
        print(f"✅ {func} -> async (padrão 2)")

# 4. Remove duplicações acidentais
content = content.replace('async async def', 'async def')

# 5. Grava o arquivo modificado em UTF‑8
with open('main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print(f"\n✨ {fixes} funções corrigidas!")
print("🚀 Pronto para deploy!")
print("\nPróximos passos:")
print("1. git add main.py")
print("2. git commit -m 'Fix: async functions and imports'")
print("3. git push")
