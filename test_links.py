import httpx
import asyncio

async def testar_fontes():
    fontes = {
        "G1 RSS": "https://g1.globo.com/rss/mg/minas-gerais/",
        "Palácio das Artes": "https://fcs.mg.gov.br/programacao/",
        "Sympla BH": "https://www.sympla.com.br/eventos/belo-horizonte-mg"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9"
    }

    print(f"{'FONTE':<20} | {'STATUS':<7} | {'RESPOSTA'}")
    print("-" * 60)

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=20.0) as client:
        for nome, url in fontes.items():
            try:
                resp = await client.get(url)
                # Verifica se o conteúdo parece válido (não vazio)
                preview = resp.text[:50].replace('\n', '')
                print(f"{nome:<20} | {resp.status_code:<7} | {preview}...")
                
                if resp.status_code == 400:
                    print(f"   👉 Erro 400 em {nome}: O servidor rejeitou nossos headers.")
            except Exception as e:
                print(f"{nome:<20} | ERROR   | {str(e)}")

if __name__ == "__main__":
    asyncio.run(testar_fontes())
