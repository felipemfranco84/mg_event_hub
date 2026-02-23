"""
Padrão de Qualidade: Professional API Gateway (v8.6.5).
Motivo: Sincronização total com EventoModel e Dashboard FullCalendar.
"""
import os
from fastapi import FastAPI, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from datetime import datetime

from app.core.database import get_session, init_db
from app.models import EventoModel

app = FastAPI(title="MG Event Hub - Professional Dashboard")

# Configuração de Templates
templates = Jinja2Templates(directory="app/templates")

@app.on_event("startup")
async def startup():
    """Garante que o banco esteja pronto ao ligar o servidor."""
    await init_db()

@app.get("/")
async def read_root(request: Request):
    """Renderiza o Dashboard Profissional."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/eventos")
async def get_eventos(session: AsyncSession = Depends(get_session)):
    """
    Rota de alimentação do FullCalendar e Radar Lateral.
    Entrega datas formatadas em ISO para o calendário acender.
    """
    try:
        query = select(EventoModel).order_by(EventoModel.data_evento.asc())
        result = await session.execute(query)
        eventos_db = result.scalars().all()

        eventos_formatados = []
        for e in eventos_db:
            eventos_formatados.append({
                "id": e.id_unico,
                "title": e.titulo,
                "start": e.data_evento.strftime("%Y-%m-%d"),
                "cidade": e.cidade,
                "preco_base": float(e.preco_base),
                "vibe": e.vibe,
                "url": e.url_origem,
                "local": e.local
            })
        
        return eventos_formatados
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)