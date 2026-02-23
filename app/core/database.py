"""
Justificativa: Garantia de Singleton do Engine e criação de tabelas síncronas com o Modelo.
"""
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.core.logger import log
from app.models import Base, EventoModel # ✅ Import obrigatório

DB_DIR = Path("data")
DB_DIR.mkdir(exist_ok=True)
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_DIR}/mg_events.db"

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("🚀 Database v9.0: Tabelas mapeadas e prontas.")
    except Exception as e:
        log.error(f"❌ Erro crítico no banco: {e}")

async def get_session():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()