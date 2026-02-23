"""
Padrão de Qualidade: Web Serving & API.
"""
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from app.core.database import init_db
from app.core.scheduler import start_scheduler, scheduler
from app.routers import eventos
from app.core.logger import log

templates = Jinja2Templates(directory="app/templates")

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Iniciando MG-Event-Hub...")
    await init_db()
    start_scheduler()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan, root_path="/mg_event_hub")
app.include_router(eventos.router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
