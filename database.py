# database.py
import os
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  Monta DSN assíncrono + SSL  ────────────────
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — confira o render.yaml")

# Força o driver asyncpg
if raw.startswith("postgres://"):
    raw = re.sub(r"^postgres://", "postgresql+asyncpg://", raw, 1)
elif raw.startswith("postgresql://") and "+asyncpg" not in raw:
    raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# Normaliza query‐string → garante ssl=require (valor ACEITO pelo asyncpg)
url = urlparse(raw)
qs = parse_qs(url.query, keep_blank_values=True)

# Remove parâmetros SSL conflitantes
qs.pop("ssl", None)
qs.pop("sslmode", None)

# asyncpg usa "ssl=require" ao invés de "sslmode=require"
qs["ssl"] = ["require"]

raw_async = urlunparse(url._replace(query=urlencode(qs, doseq=True)))
print("🔌 DSN usado pelo SQLAlchemy →", raw_async)

# ──────────────────────  Engine & Session  ───────────────────
engine = create_async_engine(
    raw_async,
    echo=False,
    pool_size=5,
    max_overflow=10,
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency de injeção para FastAPI, tipada corretamente."""
    async with AsyncSessionLocal() as session:
        yield session