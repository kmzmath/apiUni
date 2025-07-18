from pydantic import BaseModel, field_serializer, field_validator, UUID4, ConfigDict, Field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

class Estado(BaseModel):
    id: int
    sigla: str
    nome: str
    icone: str | None = None
    regiao: str
    
    model_config = ConfigDict(from_attributes=True)

class Team(BaseModel):
    id: int
    name: str
    logo: str | None = None
    tag: str | None = None
    slug: str | None = None
    university: str | None = None
    university_tag: str | None = None
    estado: str | None = None
    instagram: str | None = None
    twitch: str | None = None

    model_config = ConfigDict(from_attributes=True)

class TeamMinimal(BaseModel):
    id: int
    name: str
    tag: str | None = None
    logo: str | None = None
    estado_sigla: str | None = None
    estado_icone: str | None = None
    
    model_config = ConfigDict(from_attributes=True)

class Tournament(BaseModel):
    id: UUID4
    name: str
    logo: str | None = None
    organizer: str | None = None
    starts_on: datetime | None = Field(None, alias="startsOn")
    ends_on: datetime | None = Field(None, alias="endsOn")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True  # Permite usar tanto o nome do campo quanto o alias
    )
    
    # Serializa de volta para camelCase na resposta
    @field_serializer("starts_on", return_type=str)
    def serialize_starts_on(self, dt: datetime | None):
        if dt is None:
            return None
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    
    @field_serializer("ends_on", return_type=str)
    def serialize_ends_on(self, dt: datetime | None):
        if dt is None:
            return None
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    
    # Adiciona propriedades para manter compatibilidade com a API
    @property
    def startsOn(self):
        return self.starts_on
    
    @property
    def endsOn(self):
        return self.ends_on
    
    def model_dump(self, **kwargs):
        """Override para garantir que a resposta use camelCase"""
        data = super().model_dump(**kwargs)
        # Renomeia os campos para camelCase na saída
        if 'starts_on' in data:
            data['startsOn'] = data.pop('starts_on')
        if 'ends_on' in data:
            data['endsOn'] = data.pop('ends_on')
        return data

class TeamMatchInfo(BaseModel):
    id: UUID4
    team: Team
    score: int | None = None
    agent_1: str | None = None
    agent_2: str | None = None
    agent_3: str | None = None
    agent_4: str | None = None
    agent_5: str | None = None

    model_config = ConfigDict(from_attributes=True)
    
    # Validador para tratar "?" como None
    @field_validator('agent_1', 'agent_2', 'agent_3', 'agent_4', 'agent_5', mode='before')
    @classmethod
    def validate_agent(cls, v):
        if v == '?' or v == '':
            return None
        return v

class Match(BaseModel):
    id: UUID4
    date: datetime
    map: str | None
    round: str | None = None
    tournament: Tournament | None = None
    tmi_a: TeamMatchInfo
    tmi_b: TeamMatchInfo

    model_config = ConfigDict(from_attributes=True)

    # transforma datetime → ISO automático
    @field_serializer("date", return_type=str)
    def serialize_dt(self, dt: datetime, _info):
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    
class RankingScores(BaseModel):
    colley:       Optional[float] = None
    massey:       Optional[float] = None
    elo:          Optional[float] = None
    elo_mov:      Optional[float] = None
    trueskill:    Optional[float] = None
    pagerank:     Optional[float] = None
    pca:          Optional[float] = None
    sos:          Optional[float] = None
    consistency:  Optional[float] = None
    borda:        Optional[int]   = None
    integrado:    Optional[float] = None
    bradley_terry: Optional[float] = None

class Anomaly(BaseModel):
    is_anomaly: bool
    score: Optional[float] = None


class RankingItem(BaseModel):
    posicao:        int
    team_id:        int
    team:           str
    tag:            str
    university:     Optional[str] = None

    nota_final:     float
    ci_lower:       float
    ci_upper:       float
    incerteza:      float
    games_count:    int

    variacao:       Optional[int]   = None
    variacao_nota:  Optional[float] = None
    is_new:         bool            = False

    scores:         RankingScores
    anomaly:        Optional[Anomaly] = None


class RankingResponse(BaseModel):
    ranking: list[RankingItem]
    total: int
    limit: int | None
    cached: bool
    cache_age_seconds: int | None = None
    last_update: str

class RankingStats(BaseModel):
    total_teams: int
    stats: dict
    top_5: list[RankingItem]
    last_update: str
    cached: bool

class TeamHistoryItem(BaseModel):
    date: str
    position: int
    nota_final: float
    ci_lower: float
    ci_upper: float
    games_count: int
    total_teams: int
    scores: dict

class TeamHistoryResponse(BaseModel):
    team: dict
    history: list[TeamHistoryItem]
    count: int

class TournamentPerformance(BaseModel):
    tournament: dict
    performance: dict
    participation: dict

class TeamTournamentsResponse(BaseModel):
    team: dict
    summary: dict
    active: list[TournamentPerformance]
    finished: list[TournamentPerformance]

class TeamCompleteResponse(BaseModel):
    team: dict
    roster: dict
    ranking: dict
    statistics: dict
    recent_matches: dict
    tournaments: dict
    last_updated: str

class MapRoundStats(BaseModel):
    total_played: int
    total_won: int
    total_lost: int
    avg_won_per_match: float
    avg_lost_per_match: float
    round_winrate_percent: float

class MarginMatch(BaseModel):
    """Detalhes de uma partida específica para as margens"""
    date: str
    opponent: str
    opponent_id: int
    score: str  # formato "16-9"
    tournament: Optional[str]
    tournament_id: Optional[int]

class MarginDetail(BaseModel):
    """Detalhe de uma margem com informações da partida"""
    margin: int
    match: Optional[MarginMatch]  # None se não houver partidas

class MapMargins(BaseModel):
    """Margens de vitória/derrota com detalhes das partidas"""
    biggest_win: MarginDetail
    biggest_loss: MarginDetail

class MapDates(BaseModel):
    first_played: str | None
    last_played: str | None

class RecentMapMatch(BaseModel):
    date: str
    opponent: str
    score: str
    result: str
    tournament: str | None

class MapStatistics(BaseModel):
    map_name: str
    matches_played: int
    wins: int
    losses: int
    draws: int
    playrate_percent: float
    winrate_percent: float
    rounds: MapRoundStats
    margins: MapMargins
    dates: MapDates
    recent_matches: list[RecentMapMatch]

class TeamMapStatsOverall(BaseModel):
    total_matches: int
    total_wins: int
    total_losses: int
    total_draws: int
    total_maps_played: int
    overall_winrate: float

class TeamMapStatsResponse(BaseModel):
    team_id: int
    team: dict
    overall_stats: TeamMapStatsOverall
    maps: list[MapStatistics]

class MapComparisonItem(BaseModel):
    map: str
    matches: int
    winrate: float
    round_winrate: float
    playrate: float
    performance: dict
    avg_score: dict
    rating: float

class TeamMapComparisonResponse(BaseModel):
    team: dict
    overall_winrate: float
    maps_comparison: list[MapComparisonItem]
    best_maps: list[MapComparisonItem]
    worst_maps: list[MapComparisonItem]