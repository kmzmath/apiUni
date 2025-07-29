# database.py - VERSÃO CORRIGIDA PARA PGBOUNCER
import os
import re
import ssl
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# ───────────────  Configuração para Supabase  ────────────────
raw = os.getenv("DATABASE_URL")
if not raw:
    raise RuntimeError("DATABASE_URL não definida — configure a URL do Supabase")

# Força o driver asyncpg para operações assíncronas
if raw.startswith("postgres://"):
    raw = re.sub(r"^postgres://", "postgresql+asyncpg://", raw, 1)
elif raw.startswith("postgresql://") and "+asyncpg" not in raw:
    raw = raw.replace("postgresql://", "postgresql+asyncpg://", 1)

# Remove qualquer query string existente
if "?" in raw:
    raw_async = raw.split("?")[0]
else:
    raw_async = raw

print("🔌 Conectando ao Supabase com URL:", raw_async.split('@')[0] + "@[HIDDEN]")

# Cria contexto SSL
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# ──────────────────────  Engine & Session  ───────────────────
engine = create_async_engine(
    raw_async,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "ssl": ssl_context,
        "server_settings": {
            "application_name": "valorant-api",
            "jit": "off"
        },
        "command_timeout": 60,
        # IMPORTANTE: Desabilita prepared statements para pgbouncer
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
    }
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