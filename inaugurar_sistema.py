#!/usr/bin/env python3
import asyncio
import sys
import os

# Garante que o Python encontre a pasta 'app'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import AsyncSessionLocal, init_db
from app.services.manager import DataManager
from app.core.logger import log

async def main():
    log.info("🚀 Iniciando Inauguração do MG-Event-Hub...")
    
    # Cria as tabelas se não existirem
    await init_db()

    async with AsyncSessionLocal() as session:
        try:
            manager = DataManager(session)
            log.info("🔍 Disparando scrapers manualmente...")
            total = await manager.run_all_scrapers()

            if total > 0:
                log.info(f"✅ SUCESSO! {total} eventos novos salvos no banco.")
            else:
                log.warning("⚠️ Nenhum evento novo encontrado (pode ser que já existam ou o site mudou).")
        except Exception as e:
            log.error(f"❌ Falha crítica na inauguração: {e}")

if __name__ == "__main__":
    asyncio.run(main())
