# 🎮 Valorant Universitário API

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-00a393.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-316192.svg)](https://www.postgresql.org/)

## [Clique aqui para ir para o Site](https://univlr-web.vercel.app/)

## 🛠 Tecnologias

- **Framework:** FastAPI
- **Banco de Dados:** PostgreSQL
- **ORM:** SQLAlchemy (async)
- **Validação:** Pydantic
- **Ranking:** NumPy, SciPy, scikit-learn, TrueSkill
- **Deploy:** Render.com

## 📡 Endpoints

### Times
- `GET /teams` - Lista todos os times
- `GET /teams/{team_id}` - Detalhes de um time
- `GET /teams/{team_id}/matches` - Partidas de um time
- `GET /teams/{team_id}/stats` - Estatísticas do time
- `GET /teams/{team_id}/players` - Jogadores do time

### Ranking
- `GET /ranking` - Ranking atual
- `GET /ranking/{team_id}` - Posição de um time
- `GET /ranking/snapshots` - Lista de snapshots
- `POST /ranking/snapshot` - Criar novo snapshot (admin)
- `DELETE /ranking/snapshot/{id}` - Excluir snapshot (admin)

### Partidas
- `GET /matches` - Últimas partidas
- `GET /matches/{match_id}` - Detalhes de uma partida

### Estatísticas
- `GET /stats/maps` - Estatísticas por mapa
- `GET /stats/summary` - Resumo geral

## 👥 Autores

- **kmyzth** - *API, Cálculos e Data* - [kmzmath](https://github.com/kmzmath)
- **Kick** - *Site e Front-end* - [mgruntowski](https://github.com/mgruntowski)


---

<p align="center">
  Feito com ❤️ para a Comunidade Universitária de Valorant 
</p>