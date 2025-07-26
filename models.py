# models.py
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql as pg
from typing import Optional

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Boolean, Text, UniqueConstraint, Index
from database import Base

Base = declarative_base()

class Estado(Base):
    __tablename__ = "estados"
    
    id = sa.Column(sa.Integer, primary_key=True)
    sigla = sa.Column(sa.String(2), unique=True, nullable=False)  # UF (ex: SP, RJ)
    nome = sa.Column(sa.String(50), nullable=False)  # Nome completo (ex: São Paulo)
    icone = sa.Column(sa.String(600))  # URL do ícone/bandeira do estado
    regiao = sa.Column(sa.String(20), nullable=False)  # Norte, Nordeste, Centro-Oeste, Sudeste, Sul
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                          server_default=sa.text("now()"))
    
    # Relationship
    teams = relationship("Team", back_populates="estado_obj")

class Team(Base):
    __tablename__ = "teams"

    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String(200), unique=True, nullable=False)
    name = sa.Column(sa.String(180), nullable=False)
    tag = sa.Column(sa.String(10))
    org = sa.Column(sa.String(100))
    orgTag = sa.Column(sa.String(20))
    logo = sa.Column(sa.String(600))
    
    # Jogadores
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
    
    # Redes sociais
    instagram = sa.Column(sa.String(100))
    twitch = sa.Column(sa.String(100))
    
    # Estado
    estado = sa.Column(sa.String(100))
    estado_id = sa.Column(sa.Integer, sa.ForeignKey("estados.id"))
    estado_obj = relationship("Estado", back_populates="teams")
    
    # Campos de ranking desnormalizados (para performance)
    current_ranking_position = sa.Column(sa.Integer)
    current_ranking_score = sa.Column(sa.Numeric(5, 2))
    current_ranking_games = sa.Column(sa.Integer, default=0)
    current_ranking_snapshot_id = sa.Column(sa.Integer)
    current_ranking_updated_at = sa.Column(sa.TIMESTAMP(timezone=True))
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))
    
    # Propriedades para compatibilidade
    @property
    def university(self):
        return self.org
    
    @property
    def university_tag(self):
        return self.orgTag

class Agent(Base):
    __tablename__ = "agents"
    
    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String(100), unique=True, nullable=False)
    nome_agente = sa.Column(sa.String(100), nullable=False)
    classe = sa.Column(sa.String(50), nullable=False)
    icon = sa.Column(sa.String(600))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class Map(Base):
    __tablename__ = "maps"
    
    id = sa.Column(sa.Integer, primary_key=True)
    slug = sa.Column(sa.String(100), unique=True, nullable=False)
    nome_mapa = sa.Column(sa.String(100), nullable=False)
    icon = sa.Column(sa.String(600))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class Tournament(Base):
    __tablename__ = "tournaments"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(180), unique=True, nullable=False)
    organizer = sa.Column(sa.String(100))
    start_date = sa.Column(sa.TIMESTAMP(timezone=True))
    end_date = sa.Column(sa.TIMESTAMP(timezone=True))
    logo = sa.Column(sa.String(600))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))
    
    # Propriedades para compatibilidade com a API antiga
    @property
    def starts_on(self):
        return self.start_date
    
    @property
    def ends_on(self):
        return self.end_date

class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True,
                   server_default=sa.text("uuid_generate_v4()"))
    team_slug = sa.Column(sa.String(200), 
                         sa.ForeignKey("teams.slug", ondelete="CASCADE"), 
                         nullable=False)
    score = sa.Column(sa.SmallInteger)
    agent1 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agent2 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agent3 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agent4 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agent5 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    # Relationships
    team = relationship("Team", lazy="selectin", foreign_keys=[team_slug])
    
    # Propriedade para compatibilidade (a API antiga usava team_id)
    @property
    def team_id(self):
        return self.team.id if self.team else None

class Match(Base):
    __tablename__ = "matches"

    idPartida = sa.Column(sa.String(100), primary_key=True)
    date = sa.Column(sa.Date, nullable=False)
    time = sa.Column(sa.Time, nullable=False)
    team_i = sa.Column(sa.String(200), sa.ForeignKey("teams.slug"), nullable=False)
    team_j = sa.Column(sa.String(200), sa.ForeignKey("teams.slug"), nullable=False)
    score_i = sa.Column(sa.SmallInteger, nullable=False)
    score_j = sa.Column(sa.SmallInteger, nullable=False)
    campeonato = sa.Column(sa.String(180), sa.ForeignKey("tournaments.name"))
    fase = sa.Column(sa.String(50))
    agente1 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agente2 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agente3 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agente4 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    agente5 = sa.Column(sa.String(100), sa.ForeignKey("agents.slug"))
    mapa = sa.Column(sa.String(100), sa.ForeignKey("maps.slug"))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                          server_default=sa.text("now()"))
    tmi_a = sa.Column(pg.UUID(as_uuid=True), sa.ForeignKey("team_match_info.id"))
    tmi_b = sa.Column(pg.UUID(as_uuid=True), sa.ForeignKey("team_match_info.id"))
    
    # Campos adicionais para compatibilidade
    url = sa.Column(sa.String(600))
    picks_bans = sa.Column(pg.JSONB)
    
    # Relationships
    team_i_obj = relationship("Team", foreign_keys=[team_i])
    team_j_obj = relationship("Team", foreign_keys=[team_j])
    tournament = relationship("Tournament", foreign_keys=[campeonato])
    map_obj = relationship("Map", foreign_keys=[mapa])
    
    team_match_info_a = relationship("TeamMatchInfo", foreign_keys=[tmi_a])
    team_match_info_b = relationship("TeamMatchInfo", foreign_keys=[tmi_b])
    
    # Propriedades para compatibilidade com a API antiga
    @property
    def id(self):
        return self.idPartida
    
    @property
    def tournament_id(self):
        return self.tournament.id if self.tournament else None
    
    @property
    def map(self):
        return self.mapa
    
    @property
    def round(self):
        return self.fase
    
    @property
    def datetime(self):
        from datetime import datetime
        if self.date and self.time:
            return datetime.combine(self.date, self.time)
        return None

class RankingSnapshot(Base):
    __tablename__ = "ranking_snapshots"
    
    id = sa.Column(sa.Integer, primary_key=True)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                          nullable=False,
                          server_default=sa.text("now()"))
    total_matches = sa.Column(sa.Integer, nullable=False)
    total_teams = sa.Column(sa.Integer, nullable=False)
    snapshot_metadata = sa.Column(pg.JSONB)
    
    # Relationship
    history_entries = relationship("RankingHistory", 
                                 back_populates="snapshot",
                                 cascade="all, delete-orphan")

class RankingHistory(Base):
    __tablename__ = "ranking_history"
    
    id = sa.Column(sa.Integer, primary_key=True)
    snapshot_id = sa.Column(sa.Integer, 
                           sa.ForeignKey("ranking_snapshots.id", ondelete="CASCADE"),
                           nullable=False)
    team_id = sa.Column(sa.Integer,
                       sa.ForeignKey("teams.id", ondelete="CASCADE"),
                       nullable=False)
    
    # Dados principais
    position = sa.Column(sa.Integer, nullable=False)
    nota_final = sa.Column(sa.Numeric(5, 2), nullable=False)
    ci_lower = sa.Column(sa.Numeric(5, 2), nullable=False)
    ci_upper = sa.Column(sa.Numeric(5, 2), nullable=False)
    incerteza = sa.Column(sa.Numeric(5, 2), nullable=False)
    games_count = sa.Column(sa.Integer, nullable=False)
    
    # Scores individuais
    score_colley = sa.Column(sa.Numeric(10, 6))
    score_massey = sa.Column(sa.Numeric(10, 6))
    score_elo_final = sa.Column(sa.Numeric(10, 6))
    score_elo_mov = sa.Column(sa.Numeric(10, 6))
    score_trueskill = sa.Column(sa.Numeric(10, 6))
    score_pagerank = sa.Column(sa.Numeric(10, 6))
    score_bradley_terry = sa.Column(sa.Numeric(10, 6))
    score_pca = sa.Column(sa.Numeric(10, 6))
    score_sos = sa.Column(sa.Numeric(10, 6))
    score_consistency = sa.Column(sa.Numeric(10, 6))
    score_integrado = sa.Column(sa.Numeric(10, 6))
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                          nullable=False,
                          server_default=sa.text("now()"))
    
    # Relationships
    snapshot = relationship("RankingSnapshot", back_populates="history_entries")
    team = relationship("Team")
    
    __table_args__ = (
        sa.UniqueConstraint('snapshot_id', 'team_id', 
                           name='ranking_history_snapshot_team_unique'),
    )

class TeamPlayer(Base):
    __tablename__ = "team_players"
    
    id = sa.Column(sa.Integer, primary_key=True)
    team_id = sa.Column(sa.Integer, 
                       sa.ForeignKey("teams.id", ondelete="CASCADE"), 
                       nullable=False)
    player_nick = sa.Column(sa.String(80), nullable=False)
    
    # Relationship
    team = relationship("Team")
    
    __table_args__ = (
        sa.UniqueConstraint('team_id', 'player_nick', 
                           name='team_player_unique'),
    )