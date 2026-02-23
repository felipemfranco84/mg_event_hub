"""
Padrão de Qualidade: Professional API Gateway (v8.7.0).
Correção: Unificação de rotas e sincronização total com o FullCalendar.
"""
import os
from fastapi import FastAPI, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from datetime import datetime

from app.core.database import get_session, init_db
from app.models import EventoModel
from app.core.scheduler import start_scheduler

app = FastAPI(title="MG Event Hub - v8.7")
templates = Jinja2Templates(directory="app/templates")

@app.on_event("startup")
async def startup():
    """Ciclo de vida inicial: Banco e Agendador."""
    await init_db()
    try:
        start_scheduler()
    except Exception as e:
        print(f"Aviso: Scheduler já em execução ou erro: {e}")

@app.get("/")
async def read_root(request: Request):
    """Renderiza a interface principal."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/eventos")
async def get_eventos(session: AsyncSession = Depends(get_session)):
    """
    Fonte de dados para o FullCalendar.
    Sincronizado com os campos: title e start.
    """
    try:
        query = select(EventoModel).order_by(EventoModel.data_evento.asc())
        result = await session.execute(query)
        eventos_db = result.scalars().all()

        return [
            {
                "id": e.id_unico,
                "title": e.titulo,
                "start": e.data_evento.strftime("%Y-%m-%d"),
                "cidade": e.cidade,
                "vibe": e.vibe,
                "preco_base": float(e.preco_base)
            } for e in eventos_db
        ]
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)