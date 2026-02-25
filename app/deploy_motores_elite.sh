#!/bin/bash
echo "🚀 Iniciando actualização dos Motores de Elite (v15.4.0)..."

# 1. PORTAL BH (Lógica Drupal v4.0)
cat << 'FILE_EOF' > app/services/extractors/portal_bh_service.py
import hashlib, httpx, asyncio, re
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PortalBHExtractor(BaseExtractor):
    BASE_URL = "https://portalbelohorizonte.com.br"
    URLS_HTML = ["/eventos", "/agenda", "/lazer-e-turismo/agenda-cultural"]

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        
        async with httpx.AsyncClient(timeout=25.0, headers=headers, follow_redirects=True) as client:
            for url_path in self.URLS_HTML:
                for page in range(0, 3):  # v4.0: Drupal é zero-indexed
                    url = f"{self.BASE_URL}{url_path}" + (f"?page={page}" if page > 0 else "")
                    try:
                        resp = await client.get(url)
                        if resp.status_code != 200: break
                        
                        tree = HTMLParser(resp.text)
                        
                        # Selectores reais do Drupal confirmados
                        cards = tree.css("div.views-row, div.view-content .views-row")
                        for card in cards:
                            a_node = card.css_first("a")
                            if not a_node: continue
                            
                            titulo = a_node.text(strip=True)
                            if len(titulo) < 5 or "Leia mais" in titulo: continue
                            
                            href = a_node.attributes.get("href", "")
                            url_evento = f"{self.BASE_URL}{href}" if href.startswith("/") else href
                            if not url_evento: url_evento = url
                            
                            uid = hashlib.md5(titulo.encode()).hexdigest()
                            eventos[uid] = EventoSchema(
                                id_unico=uid, titulo=titulo.upper()[:250], data_evento=datetime.now(),
                                cidade="Belo Horizonte", local="Belo Horizonte", categoria="Cultura/Lazer",
                                preco_base=0.0, url_evento=url_evento, fonte="Portal BH (Drupal v4)"
                            )
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        log.debug(f"[PortalBH] Erro em {url}: {e}")
                        break
        return list(eventos.values())
FILE_EOF

# 2. PALÁCIO DAS ARTES (Lógica WordPress v4.0)
cat << 'FILE_EOF' > app/services/extractors/palacio_artes_service.py
import hashlib, httpx, asyncio
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PalacioArtesExtractor(BaseExtractor):
    URLS = ["https://fcs.mg.gov.br/palacio-das-artes/programacao/", "https://fcs.mg.gov.br/programacao/"]

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        
        async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
            for url in self.URLS:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        tree = HTMLParser(resp.text)
                        # Selectores reais do WordPress
                        for node in tree.css("article.post, div.post, article.type-post"):
                            tit_node = node.css_first("h2.entry-title, h3, h2.elementor-heading-title a")
                            if not tit_node: continue
                            
                            titulo = tit_node.text(strip=True)
                            if len(titulo) < 5: continue
                            
                            href_node = node.css_first("a")
                            url_ev = href_node.attributes.get("href", url) if href_node else url
                            
                            uid = hashlib.md5(titulo.encode()).hexdigest()
                            eventos[uid] = EventoSchema(
                                id_unico=uid, titulo=titulo.upper()[:250], data_evento=datetime.now(),
                                cidade="Belo Horizonte", local="Palácio das Artes", categoria="Cultura",
                                preco_base=0.0, url_evento=url_ev, fonte="Palácio das Artes (WP v4)"
                            )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    log.debug(f"[PalacioArtes] Falha de rede em {url}: {e}")
        return list(eventos.values())
FILE_EOF

# 3. SYMPLA (Bypass SPA e Fallback Seguro v4.0)
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
import hashlib, httpx, asyncio, json
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class SymplaExtractor(BaseExtractor):
    URL = "https://www.sympla.com.br/eventos/belo-horizonte-mg"

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9"
        }
        
        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            try:
                resp = await client.get(self.URL)
                if resp.status_code == 200:
                    tree = HTMLParser(resp.text)
                    
                    # Tentativa 1: Extrair o estado React embutido (__NEXT_DATA__)
                    script_next = tree.css_first("script#__NEXT_DATA__")
                    if script_next:
                        try:
                            data = json.loads(script_next.text())
                            # Navegação profunda no JSON do Next.js
                            props = data.get("props", {}).get("pageProps", {})
                            itens = props.get("initialState", {}).get("events", {}).get("data", [])
                            
                            for ev in itens:
                                titulo = ev.get("name", "")
                                url_ev = ev.get("url", "")
                                if not titulo or not url_ev: continue
                                
                                uid = hashlib.md5(url_ev.encode()).hexdigest()
                                eventos[uid] = EventoSchema(
                                    id_unico=uid, titulo=titulo.upper()[:250], data_evento=datetime.now(),
                                    cidade="Belo Horizonte", local=ev.get("location", {}).get("city", "Belo Horizonte"),
                                    categoria="Sympla", preco_base=0.0, url_evento=url_ev, fonte="Sympla (NextJS)"
                                )
                        except Exception as e:
                            log.debug(f"[Sympla] Falha ao parsear __NEXT_DATA__: {e}")

                    # Tentativa 2: Scraping de a.EventCard (Fallback)
                    if not eventos:
                        for card in tree.css("a[href*='/evento/']"):
                            titulo_node = card.css_first("h3, div[class*='EventCard__EventTitle']")
                            if not titulo_node: continue
                            
                            titulo = titulo_node.text(strip=True)
                            url_ev = card.attributes.get("href", "")
                            if not url_ev.startswith("http"): url_ev = f"https://www.sympla.com.br{url_ev}"
                            
                            uid = hashlib.md5(url_ev.encode()).hexdigest()
                            eventos[uid] = EventoSchema(
                                id_unico=uid, titulo=titulo.upper()[:250], data_evento=datetime.now(),
                                cidade="Belo Horizonte", local="Belo Horizonte", categoria="Sympla",
                                preco_base=0.0, url_evento=url_ev, fonte="Sympla (HTML)"
                            )
            except Exception as e:
                log.error(f"[Sympla] Erro fatal: {e}")
                
        return list(eventos.values())
FILE_EOF

echo "✅ Motores de elite reconfigurados com sucesso!"
