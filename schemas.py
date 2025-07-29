# schemas.py
from pydantic import BaseModel, field_serializer, field_validator, UUID4, ConfigDict, Field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

class Team(BaseModel):
    id: int
    name: str
    logo: str | None = None
    tag: str | None = None
    slug: str | None = None
    university: str | None = None  # Mapeado de 'org'
    university_tag: str | None = None  # Mapeado de 'orgTag'
    estado: str | None = None
    instagram: str | None = None
    twitch: str | None = None

    model_config = ConfigDict(from_attributes=True)

class Tournament(BaseModel):
    id: int  # Mudou de UUID4 para int
    name: str
    logo: str | None = None
    organizer: str | None = None
    starts_on: datetime | None = Field(None, alias="start_date")
    ends_on: datetime | None = Field(None, alias="end_date")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )
    
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
    
    def model_dump(self, **kwargs):
        """Override para garantir que a resposta use camelCase"""
        data = super().model_dump(**kwargs)
        if 'starts_on' in data:
            data['startsOn'] = data.pop('starts_on')
        if 'ends_on' in data:
            data['endsOn'] = data.pop('ends_on')
        if 'start_date' in data:
            data['startsOn'] = data.pop('start_date')
        if 'end_date' in data:
            data['endsOn'] = data.pop('end_date')
        return data

class TeamMatchInfo(BaseModel):
    id: str | None = None  # UUID ou None para compatibilidade
    team: Team
    score: int | None = None

    model_config = ConfigDict(from_attributes=True)

class Match(BaseModel):
    id: str  # idPartida
    date: datetime
    map: str | None
    round: str | None = None  # fase
    tournament: Tournament | None = None
    tmi_a: TeamMatchInfo
    tmi_b: TeamMatchInfo

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("date", return_type=str)
    def serialize_dt(self, dt: datetime, _info):
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

class RankingScores(BaseModel):
    colley: Optional[float] = None
    massey: Optional[float] = None
    elo: Optional[float] = None
    elo_mov: Optional[float] = None
    trueskill: Optional[float] = None
    pagerank: Optional[float] = None
    pca: Optional[float] = None
    sos: Optional[float] = None
    consistency: Optional[float] = None
    borda: Optional[int] = None
    integrado: Optional[float] = None
    bradley_terry: Optional[float] = None

class RankingItem(BaseModel):
    posicao: int
    team_id: int
    team: str
    tag: str
    university: Optional[str] = None
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
    ranking: list[RankingItem]
    total: int
    limit: int | None
    cached: bool = False
    snapshot_id: int | None = None
    snapshot_date: str | None = None
    last_update: str | None = None

class SnapshotInfo(BaseModel):
    id: int
    created_at: str
    total_teams: int
    total_matches: int
    metadata: dict | None = None

class PlayerInfo(BaseModel):
    nick: str
    id: int