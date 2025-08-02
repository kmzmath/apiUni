# database.py - Versão robusta para pgbouncer
import os
import ssl
import certifi
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, StaticPool
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Detectar se estamos em produção (usando pgbouncer)
IS_PRODUCTION = os.getenv("ENV", "development") == "production"

# Converter URL do formato Supabase/Heroku para asyncpg se necessário
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Contexto SSL exigido pelo Supabase
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Configurações específicas para pgbouncer
if IS_PRODUCTION:
    logger.info("Configurando conexão para ambiente de produção com pgbouncer")
    
    # Criar engine assíncrono otimizado para pgbouncer
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        poolclass=NullPool,  # Sem pool no SQLAlchemy (pgbouncer faz o pooling)
        future=True,
        connect_args={
            "ssl": ssl_context,
            "statement_cache_size": 0,  # CRÍTICO: Sem prepared statements
            "prepared_statement_cache_size": 0,
            "command_timeout": 60,
            "server_settings": {
                "jit": "off",
                "application_name": "apiUni"
            }
        },
        # Configurações adicionais do pool
        pool_pre_ping=True,  # Verifica conexão antes de usar
        pool_recycle=300,    # Recicla conexões a cada 5 minutos
    )
else:
    logger.info("Configurando conexão para ambiente de desenvolvimento")
    
    # Configuração mais simples para desenvolvimento
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        future=True,
        connect_args={
            "ssl": "require" if DATABASE_URL else None,
            "server_settings": {
                "application_name": "apiUni-dev"
            }
        },
    )

# Session factory
async_session = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Dependency para FastAPI
async def get_db():
    """Fornece uma sessão por request e garante o fechamento."""
    async with async_session() as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Erro na sessão do banco: {str(e)}")
            await session.rollback()
            raise
        finally:
            await session.close()

# Função auxiliar para testar a conexão
async def test_connection():
    """Testa a conexão com o banco de dados"""
    try:
        async with async_session() as session:
            result = await session.execute("SELECT 1")
            return result.scalar() == 1
    except Exception as e:
        logger.error(f"Erro ao testar conexão: {str(e)}")
        return False