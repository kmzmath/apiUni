# database.py
import os
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  DSN síncrono + SSL (Supabase)  ────────────────
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — verifique o .env ou variável de ambiente")

# Ajusta para postgresql://
if raw.startswith("postgres://"):
    raw = raw.replace("postgres://", "postgresql://", 1)

# Força SSL para Supabase
url = urlparse(raw)
qs = parse_qs(url.query, keep_blank_values=True)
qs.pop("ssl", None)
qs.pop("sslmode", None)
qs["sslmode"] = ["require"]

DATABASE_URL = urlunparse(url._replace(query=urlencode(qs, doseq=True)))

print("🔌 SQLAlchemy DSN pronto (host:", url.hostname, ")")

# ──────────────────────  Engine & Session  ───────────────────
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()