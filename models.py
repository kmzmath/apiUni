# models.py
import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql as pg
from typing import Optional

from database import Base

class Team(Base):
    __tablename__ = "teams"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(180), nullable=False)
    logo = sa.Column(sa.String(600))
    tag = sa.Column(sa.String(10))
    slug = sa.Column(sa.String(200), unique=True)
    university = sa.Column("org", sa.String(200))  # Mapeia org -> university
    university_tag = sa.Column("orgTag", sa.String(20))  # Mapeia orgTag -> university_tag
    
    estado = sa.Column(sa.String(100))
    estado_id = sa.Column(sa.Integer, sa.ForeignKey("estados.id"))
    estado_obj = relationship("Estado", back_populates="teams")
    
    instagram = sa.Column(sa.String(100))
    twitch = sa.Column(sa.String(100))
    
    # Campos de ranking atual (desnormalizados para performance)
    current_ranking_position = sa.Column(sa.Integer)
    current_ranking_score = sa.Column(sa.Numeric(5, 2))
    current_ranking_games = sa.Column(sa.Integer, default=0)
    current_ranking_snapshot_id = sa.Column(sa.Integer)
    current_ranking_updated_at = sa.Column(sa.TIMESTAMP(timezone=True))
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class Tournament(Base):
    __tablename__ = "tournaments"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(180), nullable=False)
    logo = sa.Column(sa.String(600))
    organizer = sa.Column(sa.String(100))
    # Mapeia para as colunas corretas no banco Supabase
    starts_on = sa.Column('start_date', sa.TIMESTAMP(timezone=True))
    ends_on = sa.Column('end_date', sa.TIMESTAMP(timezone=True))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    team_id = sa.Column(sa.Integer, sa.ForeignKey("teams.id", ondelete="CASCADE"))
    team_slug = sa.Column(sa.String(200))  # Campo adicional no Supabase
    score = sa.Column(sa.Integer)
    agent_1 = sa.Column("agent1", sa.String(40))  # Mapeia agent1 -> agent_1
    agent_2 = sa.Column("agent2", sa.String(40))
    agent_3 = sa.Column("agent3", sa.String(40))
    agent_4 = sa.Column("agent4", sa.String(40))
    agent_5 = sa.Column("agent5", sa.String(40))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    team = relationship("Team", lazy="selectin")

class Match(Base):
    __tablename__ = "matches"

    id = sa.Column("idPartida", pg.UUID(as_uuid=True), primary_key=True)
    tournament_id = sa.Column(pg.UUID(as_uuid=True),
                              sa.ForeignKey("tournaments.id"))
    date = sa.Column(sa.Date, nullable=False)
    time = sa.Column(sa.Time)
    map = sa.Column("mapa", sa.String(40))
    round = sa.Column("fase", sa.String(40))
    team_match_info_a = sa.Column("tmi_a", pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"), nullable=False)
    team_match_info_b = sa.Column("tmi_b", pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"), nullable=False)
    url = sa.Column(sa.String(500))  # Campo adicional se existir
    picks_bans = sa.Column(pg.JSONB)  # Campo adicional se existir
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                          server_default=sa.text("now()"))

    tmi_a = relationship("TeamMatchInfo",
                         foreign_keys=[team_match_info_a],
                         lazy="selectin")
    tmi_b = relationship("TeamMatchInfo",
                         foreign_keys=[team_match_info_b],
                         lazy="selectin")
    tournament = relationship("Tournament", lazy="selectin")

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
    score_borda = sa.Column(sa.Integer)  # Adicionar se necessário
    
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