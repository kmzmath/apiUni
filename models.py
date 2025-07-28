# models.py
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql as pg
from datetime import datetime, date, time

from database import Base

Base = declarative_base()

class Estado(Base):
    __tablename__ = "estados"
    
    id = sa.Column(sa.Integer, primary_key=True)
    sigla = sa.Column(sa.String(2), unique=True, nullable=False)
    nome = sa.Column(sa.String(50), nullable=False)
    icone = sa.Column(sa.String(600))
    regiao = sa.Column(sa.String(20), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    
    # Relationship
    teams = relationship("Team", back_populates="estado_obj")

class Team(Base):
    __tablename__ = "teams"

    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String(200), unique=True, nullable=False)
    name = sa.Column(sa.String(180), nullable=False)
    tag = sa.Column(sa.String(10))
    org = sa.Column(sa.String(200))  # Era university
    orgTag = sa.Column(sa.String(20))  # Era university_tag
    logo = sa.Column(sa.String(600))
    
    # Players diretamente nas colunas
    player1 = sa.Column(sa.String(80))
    player2 = sa.Column(sa.String(80))
    player3 = sa.Column(sa.String(80))
    player4 = sa.Column(sa.String(80))
    player5 = sa.Column(sa.String(80))
    player6 = sa.Column(sa.String(80))
    player7 = sa.Column(sa.String(80))
    player8 = sa.Column(sa.String(80))
    player9 = sa.Column(sa.String(80))
    player10 = sa.Column(sa.String(80))
    
    estado = sa.Column(sa.String(100))
    estado_id = sa.Column(sa.Integer, sa.ForeignKey("estados.id"))
    estado_obj = relationship("Estado", back_populates="teams")
    
    instagram = sa.Column(sa.String(100))
    twitch = sa.Column(sa.String(100))
    
    # Campos de ranking desnormalizado
    current_ranking_position = sa.Column(sa.Integer)
    current_ranking_score = sa.Column(sa.Numeric(5, 2))
    current_ranking_games = sa.Column(sa.Integer, default=0)
    current_ranking_snapshot_id = sa.Column(sa.Integer)
    current_ranking_updated_at = sa.Column(sa.TIMESTAMP(timezone=True))
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    
    # Propriedades para compatibilidade com a API antiga
    @property
    def university(self):
        return self.org
    
    @property
    def university_tag(self):
        return self.orgTag

class Tournament(Base):
    __tablename__ = "tournaments"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(180), unique=True, nullable=False)
    organizer = sa.Column(sa.String(100))
    start_date = sa.Column(sa.TIMESTAMP(timezone=True))
    end_date = sa.Column(sa.TIMESTAMP(timezone=True))
    logo = sa.Column(sa.String(600))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    
    # Propriedades para compatibilidade
    @property
    def starts_on(self):
        return self.start_date
    
    @property
    def ends_on(self):
        return self.end_date

class Agent(Base):
    __tablename__ = "agents"
    
    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String, unique=True, nullable=False)
    nome_agente = sa.Column(sa.String, nullable=False)
    classe = sa.Column(sa.String, nullable=False)
    icon = sa.Column(sa.String)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))

class Map(Base):
    __tablename__ = "maps"
    
    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String, unique=True, nullable=False)
    nome_mapa = sa.Column(sa.String, nullable=False)
    icon = sa.Column(sa.String)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))

class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True, server_default=sa.text("uuid_generate_v4()"))
    team_slug = sa.Column(sa.String, sa.ForeignKey("teams.slug"), nullable=False)
    score = sa.Column(sa.SmallInteger)
    agent1 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agent2 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agent3 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agent4 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agent5 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    
    # Relationship
    team = relationship("Team", foreign_keys=[team_slug], primaryjoin="TeamMatchInfo.team_slug==Team.slug")

class Match(Base):
    __tablename__ = "matches"

    idPartida = sa.Column(sa.String, primary_key=True)
    date = sa.Column(sa.Date, nullable=False)
    time = sa.Column(sa.Time, nullable=False)
    team_i = sa.Column(sa.String, sa.ForeignKey("teams.slug"), nullable=False)
    team_j = sa.Column(sa.String, sa.ForeignKey("teams.slug"), nullable=False)
    score_i = sa.Column(sa.SmallInteger, nullable=False)
    score_j = sa.Column(sa.SmallInteger, nullable=False)
    campeonato = sa.Column(sa.String, sa.ForeignKey("tournaments.name"))
    fase = sa.Column(sa.String)
    agente1 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agente2 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agente3 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agente4 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    agente5 = sa.Column(sa.String, sa.ForeignKey("agents.slug"))
    mapa = sa.Column(sa.String, sa.ForeignKey("maps.slug"))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    tmi_a = sa.Column(pg.UUID(as_uuid=True), sa.ForeignKey("team_match_info.id"))
    tmi_b = sa.Column(pg.UUID(as_uuid=True), sa.ForeignKey("team_match_info.id"))
    
    # Relationships - SOLUÇÃO ALTERNATIVA
    team_a = relationship(
        "Team", 
        foreign_keys=[team_i], 
        primaryjoin="Match.team_i==Team.slug"
    )
    team_b = relationship(
        "Team", 
        foreign_keys=[team_j], 
        primaryjoin="Match.team_j==Team.slug"
    )
    tournament = relationship(
        "Tournament", 
        foreign_keys=[campeonato], 
        primaryjoin="Match.campeonato==Tournament.name"
    )
    map_obj = relationship(
        "Map", 
        foreign_keys=[mapa], 
        primaryjoin="Match.mapa==Map.slug"
    )
    
    # CORREÇÃO ALTERNATIVA: Use uma sintaxe diferente para as relações problemáticas
    tmi_a_obj = relationship(
        "TeamMatchInfo",
        primaryjoin="foreign(Match.tmi_a)==TeamMatchInfo.id",
        uselist=False
    )
    tmi_b_obj = relationship(
        "TeamMatchInfo",
        primaryjoin="foreign(Match.tmi_b)==TeamMatchInfo.id",
        uselist=False
    )
    
    # Propriedades para compatibilidade
    @property
    def id(self):
        return self.idPartida
    
    @property
    def tournament_id(self):
        return None  # Não temos ID, apenas nome
    
    @property
    def round(self):
        return self.fase
    
    @property
    def map(self):
        return self.map_obj.nome_mapa if self.map_obj else self.mapa
    
    @property
    def team_match_info_a(self):
        return self.tmi_a
    
    @property
    def team_match_info_b(self):
        return self.tmi_b
    
    @property
    def datetime(self):
        """Combina date e time em um datetime"""
        if self.date and self.time:
            from datetime import datetime
            return datetime.combine(self.date, self.time)
        return None
    
    # Alias para compatibilidade
    @property
    def tmi_a(self):
        return self.tmi_a_obj
    
    @property
    def tmi_b(self):
        return self.tmi_b_obj

class TeamPlayer(Base):
    __tablename__ = "team_players"
    
    id = sa.Column(sa.Integer, primary_key=True)
    team_id = sa.Column(sa.Integer, sa.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    player_nick = sa.Column(sa.String(80), nullable=False)
    
    # Relationship
    team = relationship("Team")

class RankingSnapshot(Base):
    __tablename__ = "ranking_snapshots"
    
    id = sa.Column(sa.Integer, primary_key=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()"))
    total_matches = sa.Column(sa.Integer, nullable=False)
    total_teams = sa.Column(sa.Integer, nullable=False)
    snapshot_metadata = sa.Column(pg.JSONB)
    
    # Relationship
    history_entries = relationship("RankingHistory", back_populates="snapshot", cascade="all, delete-orphan")

class RankingHistory(Base):
    __tablename__ = "ranking_history"
    
    id = sa.Column(sa.Integer, primary_key=True)
    snapshot_id = sa.Column(sa.Integer, sa.ForeignKey("ranking_snapshots.id", ondelete="CASCADE"), nullable=False)
    team_id = sa.Column(sa.Integer, sa.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    
    # Dados principais
    position = sa.Column(sa.Integer, nullable=False)
    nota_final = sa.Column(sa.Numeric, nullable=False)
    ci_lower = sa.Column(sa.Numeric, nullable=False)
    ci_upper = sa.Column(sa.Numeric, nullable=False)
    incerteza = sa.Column(sa.Numeric, nullable=False)
    games_count = sa.Column(sa.Integer, nullable=False)
    
    # Scores individuais
    score_colley = sa.Column(sa.Numeric)
    score_massey = sa.Column(sa.Numeric)
    score_elo_final = sa.Column(sa.Numeric)
    score_elo_mov = sa.Column(sa.Numeric)
    score_trueskill = sa.Column(sa.Numeric)
    score_pagerank = sa.Column(sa.Numeric)
    score_bradley_terry = sa.Column(sa.Numeric)
    score_pca = sa.Column(sa.Numeric)
    score_sos = sa.Column(sa.Numeric)
    score_consistency = sa.Column(sa.Numeric)
    score_integrado = sa.Column(sa.Numeric)
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()"))
    
    # Relationships
    snapshot = relationship("RankingSnapshot", back_populates="history_entries")
    team = relationship("Team")