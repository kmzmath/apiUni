# database.py
import os, re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  DSN assíncrono + SSL (Supabase)  ────────────────
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — verifique o .env ou variável de ambiente")

# driver asyncpg
if raw.startswith("postgres://"):
    raw = re.sub(r"^postgres://", "postgresql+asyncpg://", raw, 1)
elif raw.startswith("postgresql://") and "+asyncpg" not in raw:
    raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# força ssl=require (asyncpg aceita)
url = urlparse(raw)
qs  = parse_qs(url.query, keep_blank_values=True)
qs.pop("ssl", None)
qs.pop("sslmode", None)
qs["ssl"] = ["require"]

raw_async = urlunparse(url._replace(query=urlencode(qs, doseq=True)))

# evite logar a senha em produção
print("🔌 SQLAlchemy DSN pronto (host:", url.hostname, ")")

# ──────────────────────  Engine & Session  ───────────────────
engine = create_async_engine(
    raw_async,
    echo=False,          # mude para True em debug se quiser ver SQL
    pool_size=5,
    max_overflow=10,
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session