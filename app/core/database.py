"""
Justificativa Técnica: Garantir que o EventoModel seja carregado antes do create_all.
"""
import os
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.core.logger import log
# ✅ VITAL: Importar a Base e o Modelo aqui para o metadata funcionar
from app.models import Base, EventoModel

Path("data").mkdir(exist_ok=True)
DB_PATH = "data/mg_events.db"
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_PATH}"

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    """Cria as tabelas no SQLite."""
    try:
        async with engine.begin() as conn:
            # ✅ Agora o metadata contém a definição da tabela 'eventos'
            await conn.run_sync(Base.metadata.create_all)
        log.info("🚀 Tabelas criadas com sucesso no banco de dados.")
    except Exception as e:
        log.error(f"❌ Erro ao inicializar tabelas: {e}")
        raise

async def get_session():
    """Provider de sessão para o FastAPI."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()