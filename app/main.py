"""
Padrão de Qualidade: Solid & Clean Architecture (v9.2.0).
Motivo: Centralização das rotas principais para evitar conflitos de pathing em produção (Nginx).
Implementação de parâmetros de data (start/end) para suportar o lazy-loading do FullCalendar.
"""
import os
from datetime import datetime
from fastapi import FastAPI, Request, Depends, Query
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pathlib import Path

from app.core.database import get_session, init_db
from app.models import EventoModel
from app.core.scheduler import start_scheduler
from app.core.logger import log

# Garante a existência das pastas vitais para o sistema operar sem quebras
Path("data").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

app = FastAPI(
    title="MG Event Hub Pro",
    description="Motor de Inteligência Turística e Eventos de Minas Gerais",
    version="9.2.0"
)

templates = Jinja2Templates(directory="app/templates")

@app.on_event("startup")
async def startup_event():
    """
    Motivo: Inicializar recursos bloqueantes e agendamentos assim que o servidor subir.
    """
    log.info("🚀 Inicializando MG Event Hub v9.2.0...")
    await init_db()
    try:
        start_scheduler()
        log.info("✅ Scheduler de extração ativado com sucesso.")
    except Exception as e:
        log.error(f"❌ Falha ao iniciar o scheduler: {e}")

@app.get("/")
async def read_root(request: Request):
    """
    Motivo: Ponto de entrada da aplicação que serve o SPA (Single Page Application).
    """
    try:
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception as e:
        log.error(f"❌ Erro ao renderizar template principal: {e}")
        return {"error": "Falha ao carregar a interface."}

@app.get("/eventos")
async def get_eventos(
    session: AsyncSession = Depends(get_session),
    start: str = Query(None, description="Data inicial enviada pelo FullCalendar (ISO 8601)"),
    end: str = Query(None, description="Data final enviada pelo FullCalendar (ISO 8601)"),
    cidade: str = Query(None, description="Filtro opcional por cidade"),
    vibe: str = Query(None, description="Filtro opcional por categoria (show, pousada)")
):
    """
    Motivo: Fornecer os dados de forma estruturada para consumo via JS (Radar e Calendário).
    Trata os parâmetros nativos do FullCalendar para evitar sobrecarga de dados no cliente.
    """
    try:
        query = select(EventoModel)
        
        # Aplicação de filtros contextuais
        if cidade:
            query = query.where(EventoModel.cidade.ilike(f"%{cidade}%"))
        if vibe:
            query = query.where(EventoModel.vibe == vibe)
        
        # Filtros de janela de tempo (Performance lazy-loading)
        if start:
            try:
                # O FullCalendar envia datas como '2026-02-01T00:00:00-03:00'
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00')).replace(tzinfo=None)
                query = query.where(EventoModel.data_evento >= start_dt)
            except Exception as dt_err:
                log.warning(f"⚠️ Erro ao parsear data 'start' ({start}): {dt_err}")
                
        if end:
            try:
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00')).replace(tzinfo=None)
                query = query.where(EventoModel.data_evento <= end_dt)
            except Exception as dt_err:
                log.warning(f"⚠️ Erro ao parsear data 'end' ({end}): {dt_err}")
        
        query = query.order_by(EventoModel.data_evento.asc())
        result = await session.execute(query)
        eventos_db = result.scalars().all()
        
        # Formatação rígida para evitar erros de renderização no Frontend
        return [
            {
                "id": e.id_unico,
                "title": e.titulo,
                "start": e.data_evento.strftime("%Y-%m-%d"),
                "end": e.data_evento.strftime("%Y-%m-%d"), # Requisito FullCalendar
                "cidade": e.cidade,
                "local": e.local,
                "preco_base": float(e.preco_base) if e.preco_base else 0.0,
                "vibe": e.vibe,
                "url": e.url_origem,
                "fonte": e.fonte,
                "allDay": True
            }
            for e in eventos_db
        ]
        
    except Exception as e:
        log.error(f"❌ Erro crítico no endpoint /eventos: {e}")
        # Retornamos array vazio garantindo que o JS não quebre com 'Unexpected token <'
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)