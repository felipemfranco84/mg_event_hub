"""
Padrão: MG Event Hub Professional v9.0.
"""
from fastapi import FastAPI, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.core.database import get_session, init_db
from app.models import EventoModel
from app.core.scheduler import start_scheduler

app = FastAPI(title="MG Event Hub Pro")
templates = Jinja2Templates(directory="app/templates")

@app.on_event("startup")
async def startup():
    await init_db()
    start_scheduler()

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/eventos")
async def api_eventos(session: AsyncSession = Depends(get_session)):
    """API padronizada para FullCalendar e Radar Lateral."""
    try:
        query = select(EventoModel).order_by(EventoModel.data_evento.asc())
        result = await session.execute(query)
        rows = result.scalars().all()
        
        # ✅ Retorno sempre em LISTA para evitar erro de parse no JS
        return [
            {
                "id": r.id_unico,
                "title": r.titulo,
                "start": r.data_evento.strftime("%Y-%m-%d"),
                "cidade": r.cidade,
                "preco_base": float(r.preco_base),
                "vibe": r.vibe,
                "url": r.url_origem,
                "local": r.local
            } for r in rows
        ]
    except Exception as e:
        return {"error": str(e), "data": []}