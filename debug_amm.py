import asyncio
import httpx
from selectolax.parser import HTMLParser

async def debug():
    url = "https://www.diariomunicipal.com.br/amm-mg/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        print(f"📡 Acessando {url}...")
        resp = await client.get(url)
        print(f"📊 Status Code: {resp.status_code}")
        print(f"📦 Tamanho do HTML: {len(resp.text)} bytes")
        
        tree = HTMLParser(resp.text)
        
        print("\n🔗 --- LINKS ENCONTRADOS (Primeiros 15) ---")
        links = tree.css("a")
        for a in links[:15]:
            print(f"Texto: {a.text().strip()[:20]} | Href: {a.attributes.get('href')}")
            
        with open("amostra_amm.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("\n💾 Arquivo 'amostra_amm.html' salvo para inspeção.")

if __name__ == "__main__":
    asyncio.run(debug())
