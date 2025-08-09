from pydantic import BaseModel, ConfigDict, Field, field_serializer
from datetime import datetime
from typing import Optional, List, Dict, Any

# ===== ESTADO =====
class EstadoInfo(BaseModel):
    """Schema para informações do estado"""
    id: int
    sigla: str
    nome: str
    icone: str
    regiao: str

    model_config = ConfigDict(from_attributes=True)

# ===== TEAMS =====
class Team(BaseModel):
    """
    Schema do Team - EXATAMENTE como o front-end espera
    IMPORTANTE: 'university' e 'university_tag', não 'org' e 'orgTag'
    """
    id: int
    name: str
    logo: str
    tag: str
    slug: str
    university: str  # Mapeado de 'org' no banco
    university_tag: str  # Mapeado de 'orgTag' no banco
    estado: str
    estado_info: Optional[EstadoInfo] = None
    instagram: str
    twitch: str

    model_config = ConfigDict(from_attributes=True)

# ===== PLAYERS =====
class Player(BaseModel):
    """Schema para jogadores"""
    id: int
    nick: str
    
    model_config = ConfigDict(from_attributes=True)

# ===== TOURNAMENTS =====
class Tournament(BaseModel):
    """
    Schema para torneios
    IMPORTANTE: usa 'startsOn' e 'endsOn', não 'start_date' e 'end_date'
    """
    id: int
    name: str
    logo: Optional[str] = None
    organizer: Optional[str] = None
    startsOn: Optional[str] = None  # ISO string
    endsOn: Optional[str] = None    # ISO string

    model_config = ConfigDict(from_attributes=True)

# ===== MATCHES =====
class TeamInMatch(BaseModel):
    """Team simplificado para uso em matches"""
    id: int
    name: str
    logo: str
    tag: str
    slug: str
    university: str
    university_tag: str
    estado: str
    estado_info: Optional[EstadoInfo] = None
    instagram: str
    twitch: str

class MatchTeamInfo(BaseModel):
    """
    Informações do time na partida
    IMPORTANTE: usa 'agent_1' até 'agent_5', não 'agent1'
    """
    id: str
    team: TeamInMatch
    score: int
    agent_1: str
    agent_2: str
    agent_3: str
    agent_4: str
    agent_5: str

    model_config = ConfigDict(from_attributes=True)

class Match(BaseModel):
    """Schema para partidas"""
    id: str
    map: str
    round: str
    date: str  # ISO string
    tmi_a: MatchTeamInfo
    tmi_b: MatchTeamInfo
    tournament: Tournament

    model_config = ConfigDict(from_attributes=True)

# ===== RANKING =====
class RankingScores(BaseModel):
    """Scores individuais dos algoritmos de ranking"""
    colley: float
    massey: float
    elo: float
    elo_mov: float
    trueskill: float
    pagerank: float
    bradley_terry: float
    pca: float
    sos: float
    consistency: float
    integrado: float

class RankingItem(BaseModel):
    """Item individual do ranking"""
    posicao: int
    team_id: int
    team: str
    tag: str
    university: str
    nota_final: float
    ci_lower: float
    ci_upper: float
    incerteza: float
    games_count: int
    variacao: Optional[int] = None
    variacao_nota: Optional[float] = None
    is_new: bool = False
    scores: RankingScores

class RankingResponse(BaseModel):
    """Resposta do endpoint /ranking"""
    cached: bool = False
    last_update: str
    limit: Optional[int] = None
    total: int
    ranking: List[RankingItem]

class RankingSnapshot(BaseModel):
    """Snapshot individual do ranking"""
    id: int
    created_at: str
    total_teams: int
    total_matches: int
    metadata: Dict[str, Any]
    ranking: List[RankingItem]

class RankingSnapshotsResponse(BaseModel):
    """
    Resposta do endpoint /ranking/snapshots
    IMPORTANTE: deve ter propriedade 'data'
    """
    data: List[RankingSnapshot]