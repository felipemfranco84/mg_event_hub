#!/usr/bin/env python3
import asyncio
import sys
import os

# Garante que o Python encontre a pasta 'app'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import AsyncSessionLocal, init_db
from app.services.manager import EventManager
from app.core.logger import log

async def executar_carga(modo="completa"):
    """
    Executa a carga baseada na escolha do usuário ou agendamento.
    """
    await init_db()
    
    async with AsyncSessionLocal() as session:
        manager = EventManager(session)
        
        if modo == "rapida":
            log.info("⚡ Iniciando Carga Rápida (Apenas Divulgação: G1, Palácio, Sympla)...")
            # Removemos o extrator de PDF da lista temporariamente
            manager.scrapers = [s for s in manager.scrapers if "Diario" not in s.__class__.__name__]
        elif modo == "pdf":
            log.info("📄 Iniciando Carga Pesada (Apenas PDF/AMM)...")
            manager.scrapers = [s for s in manager.scrapers if "Diario" in s.__class__.__name__]
        else:
            log.info("🔄 Iniciando Carga Completa (Modo Madrugada)...")

        try:
            eventos_capturados = await manager.run_all_scrapers()
            total = len(eventos_capturados)
            log.info(f"✨ Ciclo finalizado. Total de eventos processados: {total}")
        except Exception as e:
            log.error(f"❌ Falha na carga {modo}: {e}")

def mostrar_menu():
    print("\n" + "="*40)
    print(" 🚀 MG-EVENT-HUB - PAINEL DE CONTROLE")
    print("="*40)
    print("1. Carga Rápida (G1 + Palácio + Sympla)")
    print("2. Carga Pesada (Apenas PDF - Demorado)")
    print("3. Carga Completa (Tudo)")
    print("0. Sair")
    return input("\nEscolha uma opção: ")

async def main():
    # Verifica se foi passado argumento via linha de comando (para o Cron)
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "--auto":
            await executar_carga(modo="completa")
            return

    # Se não houver argumento, abre o menu manual
    opcao = mostrar_menu()
    
    if opcao == "1":
        await executar_carga(modo="rapida")
    elif opcao == "2":
        await executar_carga(modo="pdf")
    elif opcao == "3":
        await executar_carga(modo="completa")
    else:
        print("👋 Saindo...")

if __name__ == "__main__":
    asyncio.run(main())
