"""
Padrão de Qualidade: Solid & Clean Architecture (v19.0.2).
Motivo: Uso de caminhos absolutos para resolver erro 404 causado por WorkingDirectory variável.
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.core.database import get_session, init_db
from app.models import EventoModel
from app.core.scheduler import start_scheduler
from app.core.logger import log

# CONFIGURAÇÃO DE CAMINHOS ABSOLUTOS
BASE_DIR = Path("/home/felicruel/apps/mg_event_hub")
STATIC_DIR = BASE_DIR / "app/static"
TEMPLATE_DIR = BASE_DIR / "app/templates"

# Garante que as pastas existam
STATIC_DIR.mkdir(parents=True, exist_ok=True)
(BASE_DIR / "data").mkdir(exist_ok=True)

app = FastAPI(title="MG Event Hub Pro", version="19.0.2")

# MONTAGEM BLINDADA (Caminho absoluto resolve o 404)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

@app.on_event("startup")
async def startup_event():
    await init_db()
    try:
        start_scheduler()
        log.info("✅ Sistema e Scheduler Online.")
    except Exception as e:
        log.error(f"❌ Erro no scheduler: {e}")

@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/eventos")
async def get_eventos(session: AsyncSession = Depends(get_session)):
    query = select(EventoModel).order_by(EventoModel.data_evento.asc())
    result = await session.execute(query)
    eventos_db = result.scalars().all()
    return [{"titulo": e.titulo, "fonte": e.fonte} for e in eventos_db]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
