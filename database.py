# database.py — rev. 2025‑07‑29 c
import os, ssl, re
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

raw_url = os.getenv("DATABASE_URL")
if not raw_url:
    raise RuntimeError("DATABASE_URL não definida")

# converte para driver assíncrono
if raw_url.startswith("postgres://"):
    async_url = re.sub(r"^postgres://", "postgresql+asyncpg://", raw_url, 1)
elif raw_url.startswith("postgresql://") and "+asyncpg" not in raw_url:
    async_url = re.sub(r"^postgresql://", "postgresql+asyncpg://", raw_url, 1)
else:
    async_url = raw_url

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

engine = create_async_engine(
    async_url,
    pool_size=int(os.getenv("DB_POOL_SIZE", 10)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 20)),
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "ssl": ssl_ctx,
        "statement_cache_size": 0,            # ← mantém PgBouncer feliz
        "command_timeout": 60,
        "server_settings": {"application_name": "valorant-ranking-api"},
    },
    echo=False,
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
