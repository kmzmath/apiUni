import os
import ssl
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# --------------------------------------------------------------------------- #
# 1. Monta a DATABASE_URL correta para o driver asyncpg                       #
# --------------------------------------------------------------------------- #

raw_url = os.getenv("DATABASE_URL")
if raw_url is None:
    raise RuntimeError("Variável de ambiente DATABASE_URL não definida.")

# Supabase usa o formato postgres://; precisamos do prefixo async:
if raw_url.startswith("postgres://"):
    async_url = raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
else:
    async_url = raw_url

# --------------------------------------------------------------------------- #
# 2. SSLContext – obrigatório no Supabase Cloud                               #
# --------------------------------------------------------------------------- #

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# --------------------------------------------------------------------------- #
# 3. Cria o engine assíncrono com pooling interno e PgBouncer‑safe            #
# --------------------------------------------------------------------------- #

engine = create_async_engine(
    async_url,
    pool_size=int(os.getenv("DB_POOL_SIZE", 10)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 20)),
    pool_pre_ping=True,    # detecta conexões mortas
    pool_recycle=3600,     # recicla após 1 h para evitar timeouts
    connect_args={
        "ssl": ssl_ctx,
        # >>>>> DESLIGA prepared statements quando passa pelo PgBouncer <<<<<
        "prepare_threshold": 0,      # força protocolo “simple query”
        "statement_cache_size": 0,   # opcional: não armazena cache
        "command_timeout": int(os.getenv("DB_COMMAND_TIMEOUT", 60)),
        # exemplo de metadado – visível em pg_stat_activity:
        "server_settings": {"application_name": "valorant-ranking-api"},
    },
    echo=False,            # mude para True se quiser logar SQLs
)

# --------------------------------------------------------------------------- #
# 4. Session factory & Base                                                   #
# --------------------------------------------------------------------------- #

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()

# Dependency para FastAPI
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
