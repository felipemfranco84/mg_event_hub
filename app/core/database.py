"""
Justificativa Técnica: SQLite para economia de RAM na GCP Free Tier.
Garantia de exportação da função get_session para o FastAPI.
"""
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.core.logger import log

# Caminho do banco na pasta data
DB_PATH = "data/mg_events.db"
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_PATH}"

engine = create_async_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def init_db():
    """Cria as tabelas usando a Base importada do models."""
    from app.models import Base as ModelBase
    try:
        async with engine.begin() as conn:
            await conn.run_sync(ModelBase.metadata.create_all)
        log.info("Database inicializado com sucesso.")
    except Exception as e:
        log.error(f"Erro ao inicializar o banco: {e}")
        raise

async def get_session():
    """Dependency para injeção de dependência na FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()