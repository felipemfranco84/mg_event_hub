"""
Padrão de Qualidade: Automação Inteligente.
Motivo: Garantir que a coleta de dados ocorra em horários de baixo tráfego (madrugada).
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.services.manager import DataManager
from app.core.database import AsyncSessionLocal
from app.core.logger import log

scheduler = AsyncIOScheduler()

async def executar_ciclo_extração():
    """Tarefa que abre uma sessão de banco e chama o Manager."""
    log.info("--- [JOB] Iniciando tarefa agendada de extração ---")
    async with AsyncSessionLocal() as session:
        try:
            manager = DataManager(session)
            total = await manager.run_all_scrapers()
            log.info(f"--- [JOB] Tarefa finalizada. {total} novos eventos adicionados. ---")
        except Exception as e:
            log.error(f"--- [JOB] Falha na tarefa agendada: {e} ---")

def start_scheduler():
    """Inicia o agendador e define os horários."""
    # Configurado para rodar todo dia às 03:00 AM
    scheduler.add_job(
        executar_ciclo_extração,
        CronTrigger(hour=3, minute=0),
        id="extração_diaria",
        replace_existing=True
    )
    
    scheduler.start()
    log.info("Agendador de tarefas iniciado (03:00 AM diário).")
