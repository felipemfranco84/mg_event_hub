"""
Justificativa Técnica: SQLite para economia de RAM na GCP Free Tier.
SQLAlchemy 2.0 Assíncrono para garantir que a API continue respondendo durante o scraping.
"""
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.logger import log

# Caminho do banco na pasta data que criamos
DB_PATH = "data/mg_events.db"
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_PATH}"

# Engine assíncrono
engine = create_async_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}, # Necessário para SQLite
)

# Fábrica de sessões
AsyncSessionLocal = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

# Classe Base para os Modelos
class Base(DeclarativeBase):
    pass

async def init_db():
    """Cria as tabelas se não existirem."""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("Database inicializado com sucesso.")
    except Exception as e:
        log.error(f"Erro ao inicializar o banco: {e}")
        raise

async def get_db():
    """Dependency para injeção de dependência na FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
