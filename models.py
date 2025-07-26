# models.py
import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql as pg

from database import Base   # Base já vem do database.py


# ╔═══════════════════ TEAMS ════════════════════╗
class Team(Base):
    __tablename__ = "teams"

    id   = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(180), nullable=False)
    logo = sa.Column(sa.String(600))
    tag  = sa.Column(sa.String(10))
    slug = sa.Column(sa.String(200), unique=True)

    org    = sa.Column(sa.String(200))   # universidade / organização
    orgTag = sa.Column(sa.String(20))

    estado     = sa.Column(sa.String(100))                    # texto livre (legado)
    estado_id  = sa.Column(sa.Integer, sa.ForeignKey("estados.id"))
    estado_obj = relationship("Estado", back_populates="teams")

    instagram = sa.Column(sa.String(100))
    twitch    = sa.Column(sa.String(100))

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))


# ╔══════════════════ TOURNAMENTS ═════════════════╗
class Tournament(Base):
    __tablename__ = "tournaments"

    id        = sa.Column(pg.UUID(as_uuid=True), primary_key=True)
    name      = sa.Column(sa.String(180), nullable=False)
    logo      = sa.Column(sa.String(600))
    organizer = sa.Column(sa.String(100))

    start_date = sa.Column(sa.TIMESTAMP(timezone=True))
    end_date   = sa.Column(sa.TIMESTAMP(timezone=True))

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))


# ╔════════════════ TEAM‑MATCH INFO ═══════════════╗
class TeamMatchInfo(Base):
    __tablename__ = "team_match_info"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True,
                   server_default=sa.text("uuid_generate_v4()"))

    team_slug = sa.Column(sa.String(80),
                          sa.ForeignKey("teams.slug", ondelete="CASCADE"),
                          nullable=False)

    score  = sa.Column(sa.SmallInteger)

    agent1 = sa.Column(sa.String(40))
    agent2 = sa.Column(sa.String(40))
    agent3 = sa.Column(sa.String(40))
    agent4 = sa.Column(sa.String(40))
    agent5 = sa.Column(sa.String(40))

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    team = relationship("Team",
                        lazy="selectin",
                        primaryjoin="Team.slug == TeamMatchInfo.team_slug")


# ╔════════════════════ MATCHES ═══════════════════╗
class Match(Base):
    __tablename__ = "matches"

    id = sa.Column(pg.UUID(as_uuid=True), primary_key=True,
                   server_default=sa.text("uuid_generate_v4()"))

    tournament_id = sa.Column(pg.UUID(as_uuid=True),
                              sa.ForeignKey("tournaments.id"))

    date = sa.Column(sa.Date, nullable=False)
    time = sa.Column(sa.Time, nullable=False)

    mapa = sa.Column(sa.String(40))
    fase = sa.Column(sa.String(40))

    team_match_info_a = sa.Column(pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"),
                                  nullable=False)
    team_match_info_b = sa.Column(pg.UUID(as_uuid=True),
                                  sa.ForeignKey("team_match_info.id"),
                                  nullable=False)

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    tmi_a = relationship("TeamMatchInfo",
                         foreign_keys=[team_match_info_a],
                         lazy="selectin")
    tmi_b = relationship("TeamMatchInfo",
                         foreign_keys=[team_match_info_b],
                         lazy="selectin")
    tournament = relationship("Tournament", lazy="selectin")


# ╔══════════════ RANKING SNAPSHOTS/HISTORY ════════╗
class RankingSnapshot(Base):
    __tablename__ = "ranking_snapshots"

    id            = sa.Column(sa.Integer, primary_key=True)
    created_at    = sa.Column(sa.TIMESTAMP(timezone=True),
                              server_default=sa.text("now()"))
    total_matches = sa.Column(sa.Integer, nullable=False)
    total_teams   = sa.Column(sa.Integer, nullable=False)
    snapshot_metadata = sa.Column(pg.JSONB)

    history_entries = relationship(
        "RankingHistory",
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )


class RankingHistory(Base):
    __tablename__ = "ranking_history"

    id = sa.Column(sa.Integer, primary_key=True)

    snapshot_id = sa.Column(sa.Integer,
                            sa.ForeignKey("ranking_snapshots.id", ondelete="CASCADE"),
                            nullable=False)
    team_id = sa.Column(sa.Integer,
                        sa.ForeignKey("teams.id", ondelete="CASCADE"),
                        nullable=False)

    position     = sa.Column(sa.Integer, nullable=False)
    nota_final   = sa.Column(sa.Numeric(5, 2), nullable=False)
    ci_lower     = sa.Column(sa.Numeric(5, 2), nullable=False)
    ci_upper     = sa.Column(sa.Numeric(5, 2), nullable=False)
    incerteza    = sa.Column(sa.Numeric(5, 2), nullable=False)
    games_count  = sa.Column(sa.Integer, nullable=False)

    # scores individuais
    score_colley        = sa.Column(sa.Numeric(10, 6))
    score_massey        = sa.Column(sa.Numeric(10, 6))
    score_elo_final     = sa.Column(sa.Numeric(10, 6))
    score_elo_mov       = sa.Column(sa.Numeric(10, 6))
    score_trueskill     = sa.Column(sa.Numeric(10, 6))
    score_pagerank      = sa.Column(sa.Numeric(10, 6))
    score_bradley_terry = sa.Column(sa.Numeric(10, 6))
    score_pca           = sa.Column(sa.Numeric(10, 6))
    score_sos           = sa.Column(sa.Numeric(10, 6))
    score_consistency   = sa.Column(sa.Numeric(10, 6))
    score_integrado     = sa.Column(sa.Numeric(10, 6))

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    snapshot = relationship("RankingSnapshot", back_populates="history_entries")
    team     = relationship("Team")

    __table_args__ = (
        sa.UniqueConstraint("snapshot_id", "team_id",
                            name="ranking_history_snapshot_team_unique"),
    )


# ╔══════════════════ TEAM PLAYERS ═════════════════╗
class TeamPlayer(Base):
    __tablename__ = "team_players"

    id       = sa.Column(sa.Integer, primary_key=True)
    team_id  = sa.Column(sa.Integer,
                         sa.ForeignKey("teams.id", ondelete="CASCADE"),
                         nullable=False)
    player_nick = sa.Column(sa.String(80), nullable=False)

    team = relationship("Team")

    __table_args__ = (
        sa.UniqueConstraint("team_id", "player_nick", name="team_player_unique"),
    )


# ╔════════════════════ ESTADOS ════════════════════╗
class Estado(Base):
    __tablename__ = "estados"

    id     = sa.Column(sa.Integer, primary_key=True)
    sigla  = sa.Column(sa.String(2), unique=True, nullable=False)
    nome   = sa.Column(sa.String(50), nullable=False)
    icone  = sa.Column(sa.String(600))
    regiao = sa.Column(sa.String(20), nullable=False)

    created_at = sa.Column(sa.TIMESTAMP(timezone=True),
                           server_default=sa.text("now()"))

    teams = relationship("Team", back_populates="estado_obj")