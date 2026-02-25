"""
Scanner de Força Bruta v1.0 — Detetive Omnisciente
════════════════════════════════════════════════════
Objetivo: Atacar os alvos com múltiplas identidades (evasão de WAF)
e varrer o DOM e scripts atrás de qualquer dado estruturado.
"""
import asyncio
import httpx
import json
import re
from selectolax.parser import HTMLParser

ALVOS = {
    "Sympla (HTML)": "https://www.sympla.com.br/eventos/belo-horizonte-mg",
    "Sympla (API Interna)": "https://www.sympla.com.br/api/v1/search?city=belo-horizonte-mg&only=events",
    "Portal BH (HTML)": "https://portalbelohorizonte.com.br/eventos",
    "Portal BH (JSON)": "https://portalbelohorizonte.com.br/eventos?_format=json",
    "Palácio Artes": "https://fcs.mg.gov.br/programacao/"
}

IDENTIDADES = {
    "Desktop (Chrome)": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mobile (iPhone)": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Googlebot (Bypass WAF)": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
}

SELETORES_FORCA_BRUTA = [
    "article", "div.card", "div.post", "div.evento", "div.views-row", 
    "a.EventCard", "li.evento", "div.item", "div.agenda-item"
]

async def analisar_dom(html: str):
    tree = HTMLParser(html)
    encontrados = []

    # 1. Busca por JSON-LD
    scripts_jsonld = tree.css("script[type='application/ld+json']")
    if scripts_jsonld:
        encontrados.append(f"✅ JSON-LD ENCONTRADO ({len(scripts_jsonld)} blocos)")

    # 2. Busca por NextJS/React State
    if tree.css_first("script#__NEXT_DATA__"):
        encontrados.append("✅ ESTADO NEXT.JS ENCONTRADO (__NEXT_DATA__)")

    # 3. Força Bruta CSS
    for sel in SELETORES_FORCA_BRUTA:
        nodes = tree.css(sel)
        if len(nodes) > 2: # Se achou mais de 2, pode ser uma listagem real
            encontrados.append(f"✅ SELETOR CSS FUNCIONAL: '{sel}' ({len(nodes)} elementos)")

    # 4. Busca cega por links
    links = tree.css("a")
    links_eventos = [a.attributes.get('href') for a in links if a.attributes.get('href') and ('evento' in a.attributes.get('href').lower() or 'programacao' in a.attributes.get('href').lower())]
    if links_eventos:
        encontrados.append(f"✅ LINKS DE EVENTOS DETECTADOS: {len(links_eventos)} links")

    return encontrados

async def atacar_alvo(nome: str, url: str):
    print("\n" + "═"*70)
    print(f"🎯 ALVO: {nome} | {url}")
    print("═"*70)

    sucesso = False
    html_valido = ""

    for id_nome, user_agent in IDENTIDADES.items():
        headers = {"User-Agent": user_agent, "Accept": "text/html,application/json,*/*"}
        try:
            async with httpx.AsyncClient(verify=False, follow_redirects=True, timeout=10.0) as client:
                print(f"   [>] Testando Identidade: {id_nome}...", end=" ")
                resp = await client.get(url, headers=headers)
                
                if resp.status_code == 200:
                    if len(resp.text) > 1000 or (resp.text.startswith("{") or resp.text.startswith("[")):
                        print("✅ PASSOU (Status 200 OK)")
                        sucesso = True
                        html_valido = resp.text
                        break # Se passou, não precisa tentar outras identidades
                    else:
                        print("⚠️ PASSOU, MAS VAZIO (Possível Captcha Invisível)")
                elif resp.status_code in (403, 406):
                    print(f"❌ BLOQUEADO (WAF/Cloudflare {resp.status_code})")
                else:
                    print(f"❌ FALHA (Status {resp.status_code})")
        except httpx.TimeoutException:
            print("⏳ TIMEOUT (Conexão derrubada pelo servidor)")
        except Exception as e:
            print(f"❌ ERRO HTTP: {e}")

    if sucesso and html_valido:
        print("\n   [🔍 INICIANDO VARREDURA PROFUNDA NO PAYLOAD...]")
        
        # Se for JSON puro direto da API
        if html_valido.strip().startswith("{") or html_valido.strip().startswith("["):
             print("   ✅ RESPOSTA É JSON PURO! (Brecha de API confirmada)")
             try:
                 dados = json.loads(html_valido)
                 print(f"   ✅ CHAVES ENCONTRADAS: {list(dados.keys())[:5] if isinstance(dados, dict) else 'Lista de itens'}")
             except:
                 pass
        else:
             # Se for HTML
             resultados = await analisar_dom(html_valido)
             if resultados:
                 for r in resultados:
                     print(f"   {r}")
             else:
                 print("   ⚠️ NENHUMA BRECHA ESTRUTURAL ENCONTRADA NO HTML. Site pode ser renderizado em Canvas/Iframe.")

async def main():
    print("🚀 INICIANDO MÁQUINA DE FORÇA BRUTA...\n")
    import logging
    logging.getLogger("httpx").setLevel(logging.ERROR)
    
    for nome, url in ALVOS.items():
        await atacar_alvo(nome, url)

if __name__ == "__main__":
    asyncio.run(main())
