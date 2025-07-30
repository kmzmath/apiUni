# database.py
"""
Configuração da conexão com o banco de dados PostgreSQL
"""

import os
import ssl                                 # ← faltava
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# URL do banco de dados
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Converter URL do formato Supabase/Heroku para asyncpg se necessário
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Contexto SSL exigido pelo Supabase
ssl_context = ssl.create_default_context()

# Criar engine assíncrono
engine = create_async_engine(
    DATABASE_URL,
    echo=False,          # mude para True para logar SQL
    poolclass=NullPool,  # recomendado em Render/serverless
    future=True,
    connect_args={"ssl": ssl_context},     # ← habilita SSL no asyncpg
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
        finally:
            await session.close()
