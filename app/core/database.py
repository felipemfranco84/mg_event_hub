import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.logger import log

DB_PATH = "data/mg_events.db"
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_PATH}"

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# Exportando para o Manager e Inaugurador
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def init_db():
    from app.models import EventoModel # Import local para evitar circular import
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("Database inicializado com sucesso.")

# ESSA FUNÇÃO É A QUE O MAIN.PY BUSCA
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session

# Alias para manter compatibilidade com routers se necessário
get_db = get_session