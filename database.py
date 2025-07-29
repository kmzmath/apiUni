# database.py — rev. 2025-07-29 b
import os, ssl, re
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# 1) Pega a URL bruta do ambiente
raw_url = os.getenv("DATABASE_URL")
if not raw_url:
    raise RuntimeError("A variável DATABASE_URL não está definida.")

# 2) Converte para o dialeto assíncrono, preservando tudo depois do host/porta
#    cobre três formatos: postgres://  |  postgresql://  |  postgresql+asyncpg://
if raw_url.startswith("postgres://"):
    async_url = re.sub(r"^postgres://", "postgresql+asyncpg://", raw_url, count=1)
elif raw_url.startswith("postgresql://") and "+asyncpg" not in raw_url:
    async_url = re.sub(r"^postgresql://", "postgresql+asyncpg://", raw_url, count=1)
else:  # já está correto
    async_url = raw_url

# 3) SSL – obrigatório para Supabase Cloud
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# 4) Cria o engine compatível com PgBouncer (porta 6543)
engine = create_async_engine(
    async_url,
    pool_size=int(os.getenv("DB_POOL_SIZE", 10)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 20)),
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "ssl": ssl_ctx,
        "prepare_threshold": 0,      # evita PREPARE → PgBouncer-friendly
        "statement_cache_size": 0,
        "command_timeout": 60,
        "server_settings": {"application_name": "valorant-ranking-api"},
    },
    echo=False,
)

# 5) Factory de sessão & Base
AsyncSessionLocal = sessionmaker(bind=engine,
                                 class_=AsyncSession,
                                 expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session