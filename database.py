# database.py - VERSÃO CORRIGIDA
import os
import re
import ssl
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

# Remove qualquer query string existente para evitar conflitos
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
    echo=False,  # Mude para True se quiser ver as queries SQL
    pool_size=10,  # Supabase suporta mais conexões
    max_overflow=20,
    pool_pre_ping=True,  # Verifica conexões antes de usar
    pool_recycle=3600,  # Recicla conexões a cada hora
    connect_args={
        "ssl": ssl_context,  # Passa o contexto SSL diretamente
        "server_settings": {
            "application_name": "valorant-api"
        },
        "command_timeout": 60,
        # IMPORTANTE: Desabilita o cache de prepared statements para pgbouncer
        "statement_cache_size": 0,
        # Configurações adicionais para pgbouncer
        "prepared_statement_cache_size": 0,
        "prepared_statement_name_func": lambda: None
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