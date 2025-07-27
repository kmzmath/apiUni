#!/usr/bin/env python3
"""
Script para testar as endpoints após correções
Execute com: python test_endpoints.py
"""

import requests
import json
from datetime import datetime

# Configure a URL base da sua API
BASE_URL = "https://apiuni.onrender.com/"  # Ajuste se necessário

def test_endpoint(method, path, description, params=None):
    """Testa uma endpoint e mostra o resultado"""
    print(f"\n{'='*60}")
    print(f"🧪 Testando: {description}")
    print(f"📍 {method} {path}")
    if params:
        print(f"📋 Parâmetros: {params}")
    
    try:
        if method == "GET":
            response = requests.get(f"{BASE_URL}{path}", params=params)
        else:
            response = requests.request(method, f"{BASE_URL}{path}", params=params)
        
        print(f"📊 Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                print(f"✅ Sucesso! Retornou {len(data)} itens")
                if len(data) > 0:
                    print(f"📄 Primeiro item: {json.dumps(data[0], indent=2, ensure_ascii=False)[:200]}...")
            elif isinstance(data, dict):
                print(f"✅ Sucesso! Retornou objeto")
                print(f"📄 Dados: {json.dumps(data, indent=2, ensure_ascii=False)[:300]}...")
            else:
                print(f"✅ Sucesso! Tipo de resposta: {type(data)}")
        else:
            print(f"❌ Erro HTTP {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}...")
            
    except requests.exceptions.ConnectionError:
        print("❌ Erro: Não foi possível conectar ao servidor")
        print("   Certifique-se de que o servidor está rodando!")
    except Exception as e:
        print(f"❌ Erro: {type(e).__name__}: {str(e)}")

def main():
    print("🚀 TESTE DAS ENDPOINTS DE TEAMS")
    print(f"⏰ Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 Base URL: {BASE_URL}")
    
    # Lista de testes
    tests = [
        # Teams básicos
        ("GET", "/teams", "Listar todos os times", None),
        ("GET", "/teams/56", "Buscar time por ID", None),
        ("GET", "/teams/by-slug/hunter", "Buscar time por slug", None),
        
        # Teams com complete=true (problema principal)
        ("GET", "/teams/56", "Time por ID com complete=true", {"complete": "true"}),
        ("GET", "/teams/by-slug/hunter", "Time por slug com complete=true", {"complete": "true"}),
        ("GET", "/teams/56/complete", "Endpoint /complete direto", None),
        
        # Matches e Players
        ("GET", "/teams/56/matches", "Partidas do time", None),
        ("GET", "/teams/56/players", "Jogadores do time", None),
        
        # Estados
        ("GET", "/estados", "Listar estados", None),
        ("GET", "/estados/SP/teams", "Times por estado", None),
    ]
    
    # Executa os testes
    for method, path, description, params in tests:
        test_endpoint(method, path, description, params)
    
    print(f"\n{'='*60}")
    print("✅ TESTES CONCLUÍDOS!")
    print("\n📝 CHECKLIST DE VERIFICAÇÃO:")
    print("[ ] Todos os endpoints retornam 200?")
    print("[ ] complete=true não dá mais erro 500?")
    print("[ ] /teams/{id}/matches retorna partidas?")
    print("[ ] /estados/{sigla}/teams retorna times?")
    print("[ ] /teams/{id}/players retorna jogadores?")
    
    print("\n💡 Se algum teste falhou:")
    print("1. Verifique se executou os scripts de correção")
    print("2. Reinicie o servidor FastAPI")
    print("3. Verifique os logs do servidor para mais detalhes")

if __name__ == "__main__":
    main()