import asyncio
import httpx
from selectolax.parser import HTMLParser

def extrair_slug_da_url(url: str) -> str:
    """Simula a técnica Bulletproof de transformar URL em Título."""
    try:
        # Ex: /evento/festa-da-musica/12345 -> ['evento', 'festa-da-musica', '12345']
        partes = [p for p in url.split('/') if p and not p.isdigit()]
        if partes:
            # Pega a última parte que não é número (geralmente o nome do evento)
            slug = partes[-1]
            if slug.lower() in ['evento', 'programacao', 'agenda']:
                slug = partes[-2] if len(partes) > 1 else slug
            
            # Limpa e formata
            titulo_limpo = slug.replace('-', ' ').title()
            return titulo_limpo
    except Exception:
        pass
    return "FALHA_NO_SLUG"

async def inspecionar_alvo(nome: str, url: str, seletor: str):
    print("\n" + "═"*70)
    print(f"🕵️  INSPECIONANDO DOM: {nome}")
    print("═"*70)
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                print(f"❌ Falha de rede: {resp.status_code}")
                return
                
            tree = HTMLParser(resp.text)
            nodes = tree.css(seletor)
            
            print(f"✅ Encontrados {len(nodes)} elementos com o seletor '{seletor}'\n")
            
            # Analisa apenas as 3 primeiras amostras
            for i in range(min(3, len(nodes))):
                node = nodes[i]
                
                href = node.attributes.get('href', 'SEM_HREF')
                if href == 'SEM_HREF':
                    a_interno = node.css_first('a')
                    href = a_interno.attributes.get('href', 'SEM_HREF') if a_interno else 'SEM_HREF'

                texto_bruto = node.text(strip=True)
                slug_limpo = extrair_slug_da_url(href)
                
                print(f"AMOSTRA [{i+1}]")
                print(f"  🔗 URL: {href}")
                print(f"  🗑️ Texto Sujo (Direto do HTML): '{texto_bruto[:100]}...'")
                print(f"  ✨ Título via Slug (Nossa Técnica): '{slug_limpo}'")
                print("-" * 50)
                
    except Exception as e:
        print(f"❌ Erro no teste: {e}")

async def main():
    print("🚀 INICIANDO DETETIVE DE DOM PROFUNDO...\n")
    import logging
    logging.getLogger("httpx").setLevel(logging.ERROR)
    
    await inspecionar_alvo(
        "Sympla", 
        "https://www.sympla.com.br/eventos/belo-horizonte-mg", 
        "a[href*='/evento/']"
    )
    
    await inspecionar_alvo(
        "Palácio das Artes", 
        "https://fcs.mg.gov.br/programacao/", 
        "article, div.evento"
    )

if __name__ == "__main__":
    asyncio.run(main())
