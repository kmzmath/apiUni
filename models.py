from sqlalchemy import Column, Integer, String, ForeignKey, Date, Time, SmallInteger, DateTime, JSON, Numeric, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()

class Estado(Base):
    __tablename__ = "estados"
    
    id = Column(Integer, primary_key=True)
    sigla = Column(String, nullable=False, unique=True)
    nome = Column(String, nullable=False)
    icone = Column(String)
    regiao = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relacionamentos
    teams = relationship("Team", back_populates="estado_obj")

class Team(Base):
    __tablename__ = "teams"
    
    id = Column(Integer, primary_key=True)
    slug = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False)
    tag = Column(String)
    org = Column(String)  # Será mapeado para 'university' no front
    orgTag = Column(String)  # Será mapeado para 'university_tag' no front
    logo = Column(String)
    
    # Players (deprecado - usar team_players)
    player1 = Column(String)
    player2 = Column(String)
    player3 = Column(String)
    player4 = Column(String)
    player5 = Column(String)
    player6 = Column(String)
    player7 = Column(String)
    player8 = Column(String)
    player9 = Column(String)
    player10 = Column(String)
    
    instagram = Column(String)
    twitch = Column(String)
    estado = Column(String)
    estado_id = Column(Integer, ForeignKey("estados.id"))
    
    # Campos de ranking cache
    current_ranking_position = Column(Integer)
    current_ranking_score = Column(Numeric)
    current_ranking_games = Column(Integer, default=0)
    current_ranking_snapshot_id = Column(Integer)
    current_ranking_updated_at = Column(DateTime(timezone=True))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relacionamentos
    estado_obj = relationship("Estado", back_populates="teams")
    players = relationship("TeamPlayer", back_populates="team")
    ranking_history = relationship("RankingHistory", back_populates="team")

class TeamPlayer(Base):
    __tablename__ = "team_players"
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    player_nick = Column(String, nullable=False)
    
    # Relacionamentos
    team = relationship("Team", back_populates="players")

class Tournament(Base):
    __tablename__ = "tournaments"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    organizer = Column(String)
    start_date = Column(DateTime(timezone=True))
    end_date = Column(DateTime(timezone=True))
    logo = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relacionamentos
    matches = relationship("Match", back_populates="tournament_rel", foreign_keys="Match.campeonato")

class Agent(Base):
    __tablename__ = "agents"
    
    id = Column(Integer, primary_key=True)
    slug = Column(String, nullable=False, unique=True)
    nome_agente = Column(String, nullable=False)
    classe = Column(String, nullable=False)
    icon = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Map(Base):
    __tablename__ = "maps"
    
    id = Column(Integer, primary_key=True)
    slug = Column(String, nullable=False, unique=True)
    nome_mapa = Column(String, nullable=False)
    icon = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    team_slug = Column(String, ForeignKey("teams.slug"), nullable=False)
    score = Column(SmallInteger)
    agent1 = Column(String)
    agent2 = Column(String)
    agent3 = Column(String)
    agent4 = Column(String)
    agent5 = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relacionamentos
    team = relationship("Team", foreign_keys=[team_slug], primaryjoin="TeamMatchInfo.team_slug==Team.slug")

class Match(Base):
    __tablename__ = "matches"
    
    idPartida = Column(String, primary_key=True)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    team_i = Column(String, ForeignKey("teams.slug"), nullable=False)
    team_j = Column(String, ForeignKey("teams.slug"), nullable=False)
    score_i = Column(SmallInteger, nullable=False)
    score_j = Column(SmallInteger, nullable=False)
    campeonato = Column(String, ForeignKey("tournaments.name"))
    fase = Column(String)
    mapa = Column(String, ForeignKey("maps.slug"))
    
    # Agents antigos (deprecado)
    agente1 = Column(String, ForeignKey("agents.slug"))
    agente2 = Column(String, ForeignKey("agents.slug"))
    agente3 = Column(String, ForeignKey("agents.slug"))
    agente4 = Column(String, ForeignKey("agents.slug"))
    agente5 = Column(String, ForeignKey("agents.slug"))
    
    tmi_a = Column(UUID(as_uuid=True), ForeignKey("team_match_info.id"))
    tmi_b = Column(UUID(as_uuid=True), ForeignKey("team_match_info.id"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relacionamentos
    tournament_rel = relationship("Tournament", foreign_keys=[campeonato], primaryjoin="Match.campeonato==Tournament.name")
    team_i_obj = relationship("Team", foreign_keys=[team_i], primaryjoin="Match.team_i==Team.slug")
    team_j_obj = relationship("Team", foreign_keys=[team_j], primaryjoin="Match.team_j==Team.slug")
    tmi_a_rel = relationship(
        "TeamMatchInfo", 
        foreign_keys=[tmi_a],
        primaryjoin="Match.tmi_a==TeamMatchInfo.id",
        lazy="joined"
    )
    tmi_b_rel = relationship(
        "TeamMatchInfo", 
        foreign_keys=[tmi_b],
        primaryjoin="Match.tmi_b==TeamMatchInfo.id",
        lazy="joined"
    )

class RankingSnapshot(Base):
    __tablename__ = "ranking_snapshots"
    
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    total_matches = Column(Integer, nullable=False)
    total_teams = Column(Integer, nullable=False)
    snapshot_metadata = Column(JSON)
    
    # Relacionamentos
    ranking_entries = relationship("RankingHistory", back_populates="snapshot")

class RankingHistory(Base):
    __tablename__ = "ranking_history"
    
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("ranking_snapshots.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    position = Column(Integer, nullable=False)
    nota_final = Column(Numeric, nullable=False)
    ci_lower = Column(Numeric, nullable=False)
    ci_upper = Column(Numeric, nullable=False)
    incerteza = Column(Numeric, nullable=False)
    games_count = Column(Integer, nullable=False)
    
    # Scores individuais
    score_colley = Column(Numeric)
    score_massey = Column(Numeric)
    score_elo_final = Column(Numeric)
    score_elo_mov = Column(Numeric)
    score_trueskill = Column(Numeric)
    score_pagerank = Column(Numeric)
    score_bradley_terry = Column(Numeric)
    score_pca = Column(Numeric)
    score_sos = Column(Numeric)
    score_consistency = Column(Numeric)
    score_integrado = Column(Numeric)
    
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    # Relacionamentos
    snapshot = relationship("RankingSnapshot", back_populates="ranking_entries")
    team = relationship("Team", back_populates="ranking_history")