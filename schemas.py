# schemas.py
from pydantic import BaseModel, Field, validator, UUID4
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

class Estado(BaseModel):
    id: int
    sigla: str
    nome: str
    icone: Optional[str] = None
    regiao: str
    
    class Config:
        orm_mode = True

class Team(BaseModel):
    id: int
    name: str
    logo: Optional[str] = None
    tag: Optional[str] = None
    slug: Optional[str] = None
    org: Optional[str] = None
    orgTag: Optional[str] = None
    estado: Optional[str] = None
    instagram: Optional[str] = None
    twitch: Optional[str] = None

    class Config:
        orm_mode = True

class TeamMinimal(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None
    logo: Optional[str] = None
    estado_sigla: Optional[str] = None
    estado_icone: Optional[str] = None
    
    class Config:
        orm_mode = True

class Tournament(BaseModel):
    id: UUID4
    name: str
    logo: Optional[str] = None
    organizer: Optional[str] = None
    start_date: Optional[datetime] = Field(None, alias="startsOn")
    end_date: Optional[datetime] = Field(None, alias="endsOn")

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
    
    @property
    def startsOn(self):
        return self.start_date
    
    @property
    def endsOn(self):
        return self.end_date
    
    def dict(self, **kwargs):
        """Override para garantir que a resposta use camelCase"""
        data = super().dict(**kwargs)
        # Renomeia os campos para camelCase na saída
        if 'start_date' in data:
            data['startsOn'] = data.pop('start_date')
        if 'end_date' in data:
            data['endsOn'] = data.pop('end_date')
        return data

class TeamMatchInfo(BaseModel):
    id: UUID4
    team: Team
    score: Optional[int] = None
    agent1: Optional[str] = None
    agent2: Optional[str] = None
    agent3: Optional[str] = None
    agent4: Optional[str] = None
    agent5: Optional[str] = None

    class Config:
        orm_mode = True
    
    @validator('agent1', 'agent2', 'agent3', 'agent4', 'agent5', pre=True)
    def validate_agent(cls, v):
        if v == '?' or v == '':
            return None
        return v

class Match(BaseModel):
    id: UUID4
    date: datetime 
    time: str
    mapa: Optional[str] = None
    fase: Optional[str] = None
    tournament: Optional[Tournament] = None
    tmi_a: TeamMatchInfo
    tmi_b: TeamMatchInfo

    class Config:
        orm_mode = True

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

class Anomaly(BaseModel):
    is_anomaly: bool
    score: Optional[float] = None

class RankingItem(BaseModel):
    posicao: int
    team_id: int
    team: str
    tag: str
    org: Optional[str] = None

    nota_final: float
    ci_lower: float
    ci_upper: float
    incerteza: float
    games_count: int

    variacao: Optional[int] = None
    variacao_nota: Optional[float] = None
    is_new: bool = False

    scores: RankingScores
    anomaly: Optional[Anomaly] = None

    class Config:
        orm_mode = True