import os
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.logger import log

# Cria a pasta data se não existir
Path("data").mkdir(exist_ok=True)

DB_PATH = "data/mg_events.db"
DATABASE_URL = f"sqlite+aiosqlite:///./{DB_PATH}"

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def init_db():
    from app.models import EventoModel
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("Database inicializado com sucesso.")

async def get_session():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()