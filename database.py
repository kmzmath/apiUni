# database.py - Versão corrigida para pgbouncer
import os
import ssl
import certifi
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Converter URL do formato Supabase/Heroku para asyncpg se necessário
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Adicionar parâmetros para desabilitar prepared statements na URL
if "?" in DATABASE_URL:
    DATABASE_URL += "&prepared_statement_cache_size=0&statement_cache_size=0"
else:
    DATABASE_URL += "?prepared_statement_cache_size=0&statement_cache_size=0"

logger.info("Configurando conexão com o banco de dados...")
logger.info(f"URL (sem credenciais): {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'URL não configurada'}")

# Contexto SSL exigido pelo Supabase
ssl_context = ssl.create_default_context(cafile=certifi.where())
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Criar engine assíncrono com configuração completa para pgbouncer
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    poolclass=NullPool,  # Sem pool no SQLAlchemy (pgbouncer faz o pooling)
    future=True,
    connect_args={
        "ssl": ssl_context,
        "statement_cache_size": 0,  # CRÍTICO: Desabilitar prepared statements
        "prepared_statement_cache_size": 0,
        "command_timeout": 60,
        "server_settings": {
            "jit": "off",
            "application_name": "apiUni"
        }
    },
    pool_pre_ping=True,  # Verifica conexão antes de usar
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

# Função para testar a conexão
async def test_connection():
    """Testa a conexão com o banco de dados"""
    try:
        async with async_session() as session:
            # Testa com SQL direto para verificar se prepared statements estão desabilitados
            result = await session.execute("SELECT 1")
            logger.info("✅ Conexão com banco testada com sucesso")
            return result.scalar() == 1
    except Exception as e:
        logger.error(f"❌ Erro ao testar conexão: {str(e)}")
        return False