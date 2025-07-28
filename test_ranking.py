#!/usr/bin/env python
# test_ranking.py
# Script para testar o sistema de ranking

import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Configurações
API_URL = os.getenv("API_URL", "http://localhost:8000")
ADMIN_KEY = os.getenv("ADMIN_KEY", "valorant2024admin")

def test_live_ranking():
    """Testa o cálculo ao vivo do ranking"""
    print("🔍 Testando cálculo ao vivo do ranking...")
    
    response = requests.get(f"{API_URL}/ranking/live?limit=10")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Ranking calculado com sucesso!")
        print(f"   Total de times: {data['total']}")
        print(f"   Primeiros {len(data['ranking'])} times:")
        print("-" * 60)
        
        for team in data['ranking']:
            print(f"{team['posicao']:2d}. {team['team']:30s} | Nota: {team['nota_final']:6.2f} | Jogos: {team['games_count']:3d}")
        
        return True
    else:
        print(f"❌ Erro: {response.status_code}")
        print(response.text)
        return False

def calculate_new_snapshot():
    """Calcula e salva um novo snapshot"""
    print("\n📊 Calculando novo snapshot...")
    
    response = requests.post(
        f"{API_URL}/ranking/calculate",
        params={"admin_key": ADMIN_KEY}
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Snapshot criado com sucesso!")
        print(f"   ID: {data['snapshot_id']}")
        print(f"   Times: {data['total_teams']}")
        print(f"   Partidas: {data['total_matches']}")
        print(f"   Criado em: {data['created_at']}")
        return data['snapshot_id']
    else:
        print(f"❌ Erro: {response.status_code}")
        print(response.text)
        return None

def check_saved_ranking():
    """Verifica o ranking salvo"""
    print("\n📋 Verificando ranking salvo...")
    
    response = requests.get(f"{API_URL}/ranking?limit=10")
    
    if response.status_code == 200:
        data = response.json()
        
        if data['ranking']:
            print(f"✅ Ranking do snapshot #{data['snapshot_id']}")
            print(f"   Data: {data['snapshot_date']}")
            print(f"   Total de times: {data['total']}")
            print("-" * 60)
            
            for team in data['ranking']:
                print(f"{team['posicao']:2d}. {team['team']:30s} | Nota: {team['nota_final']:6.2f}")
        else:
            print("⚠️  Nenhum ranking salvo encontrado")
        
        return True
    else:
        print(f"❌ Erro: {response.status_code}")
        print(response.text)
        return False

def compare_rankings():
    """Compara ranking ao vivo com o salvo"""
    print("\n🔄 Comparando rankings...")
    
    # Buscar ranking ao vivo
    live_response = requests.get(f"{API_URL}/ranking/live?limit=5")
    saved_response = requests.get(f"{API_URL}/ranking?limit=5")
    
    if live_response.status_code == 200 and saved_response.status_code == 200:
        live_data = live_response.json()
        saved_data = saved_response.json()
        
        print("\nTOP 5 - Comparação:")
        print("-" * 80)
        print(f"{'Pos':>3} | {'Time':30} | {'Nota (Ao Vivo)':>15} | {'Nota (Salvo)':>15}")
        print("-" * 80)
        
        for i in range(min(5, len(live_data['ranking']), len(saved_data['ranking']))):
            live = live_data['ranking'][i] if i < len(live_data['ranking']) else None
            saved = saved_data['ranking'][i] if i < len(saved_data['ranking']) else None
            
            if live and saved:
                print(f"{i+1:3d} | {live['team']:30} | {live['nota_final']:15.2f} | {saved['nota_final']:15.2f}")

def main():
    """Função principal"""
    print("🎮 TESTE DO SISTEMA DE RANKING - Valorant Universitário")
    print("=" * 60)
    
    # 1. Testar ranking ao vivo
    if not test_live_ranking():
        print("\n⚠️  Falha no teste ao vivo. Verifique se:")
        print("   - A API está rodando")
        print("   - As dependências estão instaladas (numpy, pandas, etc)")
        print("   - O arquivo ranking_calculator.py está presente")
        return
    
    # 2. Verificar ranking salvo
    check_saved_ranking()
    
    # 3. Perguntar se quer calcular novo snapshot
    print("\n" + "=" * 60)
    response = input("Deseja calcular e salvar um novo snapshot? (s/n): ")
    
    if response.lower() == 's':
        snapshot_id = calculate_new_snapshot()
        
        if snapshot_id:
            # 4. Comparar rankings
            compare_rankings()
    
    print("\n✅ Teste concluído!")

if __name__ == "__main__":
    main()