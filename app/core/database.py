"""
Padrão de Qualidade: Professional Database Core (v8.7.0).
Correção: Garantia de criação da pasta data e exportação de 'get_session'.
"""
import os
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.logger import log

# ✅ Garante que a pasta de dados existe para evitar erro de I/O
DB_DIR = Path("data")
DB_DIR.mkdir(exist_ok=True)

DB_PATH = DB_DIR / "mg_events.db"
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

class Base(DeclarativeBase):
    pass

async def init_db():
    """Inicializa as tabelas mapeadas no sistema."""
    from app.models import EventoModel
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("🚀 Database inicializado com sucesso.")
    except Exception as e:
        log.error(f"❌ Erro ao inicializar o banco: {e}")
        raise

async def get_session():
    """Dependency Injection para o FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()