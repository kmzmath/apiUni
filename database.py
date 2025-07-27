# database.py
import os
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  Configuração para Supabase  ────────────────
# Supabase fornece URLs no formato: postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT].supabase.co:5432/postgres
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — configure a URL do Supabase")

# Força o driver asyncpg para operações assíncronas
if raw.startswith("postgres://"):
    raw = re.sub(r"^postgres://", "postgresql+asyncpg://", raw, 1)
elif raw.startswith("postgresql://") and "+asyncpg" not in raw:
    raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# Parse da URL
url = urlparse(raw)
qs = parse_qs(url.query, keep_blank_values=True)

# Supabase requer SSL - garante que está configurado
qs["sslmode"] = ["require"]

# Reconstrói a URL com os parâmetros corretos
raw_async = urlunparse(url._replace(query=urlencode(qs, doseq=True)))
print("🔌 Conectando ao Supabase com URL:", raw_async.split('@')[0] + "@[HIDDEN]")

# ──────────────────────  Engine & Session  ───────────────────
engine = create_async_engine(
    raw_async,
    echo=False,  # Mude para True se quiser ver as queries SQL
    pool_size=10,  # Supabase suporta mais conexões
    max_overflow=20,
    pool_pre_ping=True,  # Verifica conexões antes de usar
    pool_recycle=3600,  # Recicla conexões a cada hora
)

AsyncSessionLocal = sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency de injeção para FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()