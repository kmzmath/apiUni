# models.py
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql as pg

Base = declarative_base()

class Team(Base):
    __tablename__ = "teams"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(180), nullable=False)
    logo = sa.Column(sa.String(600))
    tag = sa.Column(sa.String(10))
    slug = sa.Column(sa.String(200), unique=True)
    university = sa.Column(sa.String(200))
    university_tag = sa.Column(sa.String(20))
    
    # NOVO: Campos de redes sociais
    instagram = sa.Column(sa.String(100))
    twitter = sa.Column(sa.String(100))
    discord = sa.Column(sa.String(100))
    twitch = sa.Column(sa.String(100))
    youtube = sa.Column(sa.String(100))
    
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class Tournament(Base):
    __tablename__ = "tournaments"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    name = sa.Column(sa.String(180), nullable=False)
    logo = sa.Column(sa.String(600))
    organizer = sa.Column(sa.String(100))
    # Mapeia para as colunas camelCase no banco
    starts_on = sa.Column('startsOn', sa.TIMESTAMP(timezone=True))
    ends_on = sa.Column('endsOn', sa.TIMESTAMP(timezone=True))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                         server_default=sa.text("now()"))

class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"

    id        = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    team_id   = sa.Column(sa.Integer, sa.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False)
    score     = sa.Column(sa.Integer)
    agent_1   = sa.Column(sa.String(40))
    agent_2   = sa.Column(sa.String(40))
    agent_3   = sa.Column(sa.String(40))
    agent_4   = sa.Column(sa.String(40))
    agent_5   = sa.Column(sa.String(40))
    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    team = relationship("Team", lazy="selectin")

class Match(Base):
    __tablename__ = "matches"

    id                = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    tournament_id     = sa.Column(pg.UUID(as_uuid=True),
                                  sa.ForeignKey("tournaments.id"))
    date              = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    map = sa.Column(sa.String(40), nullable=True)
    round             = sa.Column(sa.String(40))
    team_match_info_a = sa.Column(pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"), nullable=False)
    team_match_info_b = sa.Column(pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"), nullable=False)
    created_at        = sa.Column(sa.TIMESTAMP(timezone=True),
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
    score_borda = sa.Column(sa.Integer)
    score_integrado = sa.Column(sa.Numeric(10, 6))
    
    # Anomalia
    is_anomaly = sa.Column(sa.Boolean, default=False)
    anomaly_score = sa.Column(sa.Numeric(10, 6))
    
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