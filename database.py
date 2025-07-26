# database.py
import os
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  Monta DSN assíncrono + SSL  ────────────────
# Para Supabase, a URL geralmente vem no formato:
# postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — confira suas variáveis de ambiente")

# Força o driver asyncpg
if raw.startswith("postgres://"):
    raw = re.sub(r"^postgres://", "postgresql+asyncpg://", raw, 1)
elif raw.startswith("postgresql://") and "+asyncpg" not in raw:
    raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# Supabase requer SSL
url = urlparse(raw)
qs = parse_qs(url.query, keep_blank_values=True)

# Remove parâmetros SSL conflitantes
qs.pop("ssl", None)
qs.pop("sslmode", None)

# asyncpg usa "ssl=require" ao invés de "sslmode=require"
qs["ssl"] = ["require"]

raw_async = urlunparse(url._replace(query=urlencode(qs, doseq=True)))
print("🔌 Conectando ao Supabase com DSN →", raw_async.split('@')[0] + '@...')  # Oculta senha no log

# ──────────────────────  Engine & Session  ───────────────────
engine = create_async_engine(
    raw_async,
    echo=False,  # Mude para True se quiser ver as queries SQL
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verifica conexões antes de usar
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency de injeção para FastAPI, tipada corretamente."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()