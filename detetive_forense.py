"""
Detetive Forense v2.0 — Inspeção de Dados e Sonda de Rede
══════════════════════════════════════════════════════════
Objetivo: Executar cada extrator isoladamente. Se retornar
0 eventos, aciona uma sonda HTTP para auditar o tráfego 
real e salvar DUMPs para engenharia reversa.
"""
import asyncio
import os
import time
import httpx

from app.services.extractors.portal_bh_service import PortalBHExtractor
from app.services.extractors.sympla_service import SymplaExtractor
from app.services.extractors.palacio_artes_service import PalacioArtesExtractor
from app.services.extractors.diario_amm_service import DiarioAMMExtractor

DEBUG_DIR = "./data/debug"
os.makedirs(DEBUG_DIR, exist_ok=True)

async def disparar_sonda(nome_motor: str, url_alvo: str):
    print(f"\n   [🔍 INICIANDO SONDA DE REDE] -> {url_alvo}")
    caminho_arquivo = os.path.join(DEBUG_DIR, f"dump_{nome_motor.lower()}.txt")
    
    # Headers simulando um navegador Chrome real
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, application/xhtml+xml, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    try:
        async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=15.0) as client:
            resp = await client.get(url_alvo, headers=headers)
            
            # Grava o DUMP físico
            with open(caminho_arquivo, "w", encoding="utf-8") as f:
                f.write(f"URL: {url_alvo}\nSTATUS: {resp.status_code}\nHEADERS: {resp.headers}\n\nBODY:\n{resp.text}")
            
            print(f"   [📡 STATUS CODE] {resp.status_code}")
            print(f"   [💾 DUMP SALVO] {caminho_arquivo}")
            
            # Mostra um pedaço do que o servidor respondeu
            snippet = resp.text[:200].replace('\n', ' ')
            print(f"   [📄 PAYLOAD RAW] {snippet}...")

            # Análise Forense
            if resp.status_code in (403, 401, 406):
                print("   [❌ DIAGNÓSTICO] BLOQUEIO WAF/CLOUDFLARE. O site detectou o bot e recusou acesso.")
            elif resp.status_code >= 500:
                print("   [❌ DIAGNÓSTICO] ERRO DE SERVIDOR. O site de destino está fora do ar.")
            elif resp.status_code == 200:
                if "event" in resp.text.lower() or "evento" in resp.text.lower():
                     print("   [⚠️ DIAGNÓSTICO] STATUS 200 COM DADOS. O bloqueio é na lógica do nosso Extrator (Regex/CSS)!")
                else:
                     print("   [⚠️ DIAGNÓSTICO] STATUS 200 VAZIO. O site carregou um desafio JS (Captcha) invisível.")

    except httpx.TimeoutException:
        print("   [❌ DIAGNÓSTICO] TIMEOUT. Servidor derrubou a conexão ou nos colocou em fila morta.")
    except Exception as e:
        print(f"   [❌ DIAGNÓSTICO] ERRO HTTP FATAL: {e}")

async def investigar_motor(scraper, test_url: str):
    nome = scraper.__class__.__name__
    print("\n" + "═"*70)
    print(f"🕵️  INVESTIGANDO MOTOR: {nome}")
    print("═"*70)
    
    inicio = time.time()
    try:
        eventos = await scraper.extract()
        tempo = time.time() - inicio
        
        if eventos:
            print(f"✅ [SUCESSO] {len(eventos)} eventos extraídos em {tempo:.2f}s.")
            print("   [AMOSTRA DOS DADOS ENCONTRADOS]:")
            # Mostra até 3 eventos para auditar a qualidade
            for i in range(min(3, len(eventos))):
                ev = eventos[i]
                tit = getattr(ev, 'titulo', ev.get('titulo') if isinstance(ev, dict) else 'Desconhecido')
                data_ev = getattr(ev, 'data_evento', ev.get('data_evento') if isinstance(ev, dict) else 'Sem data')
                print(f"     - {tit[:60]}... | {data_ev}")
        else:
            print(f"⚠️ [FALHA SILENCIOSA] O extrator retornou 0 eventos ({tempo:.2f}s). Acionando Sonda...")
            await disparar_sonda(nome, test_url)
            
    except Exception as e:
        tempo = time.time() - inicio
        print(f"❌ [CRASH NO CÓDIGO] Erro estourou durante a extração ({tempo:.2f}s): {e}")
        await disparar_sonda(nome, test_url)

async def main():
    print("🚀 INICIANDO AUDITORIA FORENSE V2.0\n")
    
    alvos = [
        (SymplaExtractor(), "https://www.sympla.com.br/api/v1/search?city=belo-horizonte-mg&only=events"),
        (PortalBHExtractor(), "https://portalbelohorizonte.com.br/eventos?_format=json"),
        (PalacioArtesExtractor(), "https://fcs.mg.gov.br/programacao/"),
        (DiarioAMMExtractor(), "https://www.diariomunicipal.com.br/amm-mg/pesquisar?q=show")
    ]
    
    for motor, url in alvos:
        await investigar_motor(motor, url)

if __name__ == "__main__":
    asyncio.run(main())
