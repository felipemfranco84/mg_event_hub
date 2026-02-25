#!/bin/bash
echo "🚀 Iniciando Recuperação Integral v15.2.0 (Motores v4.0 Reais)..."

# 1. MANAGER v4.0 (Com Migrations e Relatório)
cat << 'FILE_EOF' > app/services/manager.py
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.logger import log

from app.services.extractors.portal_bh_service import PortalBHExtractor
from app.services.extractors.sympla_service import SymplaExtractor
from app.services.extractors.palacio_artes_service import PalacioArtesExtractor
from app.services.extractors.diario_amm_service import DiarioAMMExtractor

class EventManager:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.scrapers = [
            PortalBHExtractor(),
            SymplaExtractor(),
            PalacioArtesExtractor(),
            DiarioAMMExtractor()
        ]

    async def _aplicar_migrations(self):
        try:
            await self.session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_eventos_id_unico ON eventos(id_unico)"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN imagem_url VARCHAR(500)"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN descricao TEXT"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN categoria VARCHAR(100)"))
            await self.session.commit()
        except Exception:
            await self.session.rollback()

    async def run_all_scrapers(self):
        log.info(f"🚀 Iniciando orquestrador v4.0 com {len(self.scrapers)} fontes...")
        await self._aplicar_migrations()
        todos_eventos = []
        relatorio = {}

        sql = text("""
            INSERT OR IGNORE INTO eventos 
            (id_unico, titulo, data_evento, cidade, local, descricao, categoria, preco_base, url_evento, imagem_url, fonte)
            VALUES 
            (:id_unico, :titulo, :data_evento, :cidade, :local, :descricao, :categoria, :preco_base, :url_evento, :imagem_url, :fonte)
        """)

        for scraper in self.scrapers:
            nome = scraper.__class__.__name__
            try:
                log.info(f"📡 Iniciando: {nome}")
                eventos = await scraper.extract()
                
                if not eventos:
                    relatorio[nome] = {"capturados": 0, "persistidos": 0, "erros": 0}
                    log.warning(f"⚠️ {nome}: 0 eventos.")
                    continue
                
                count_persistidos = 0
                count_erros = 0
                
                for ev in eventos:
                    try:
                        res = await self.session.execute(sql, ev.model_dump())
                        if res.rowcount > 0:
                            count_persistidos += 1
                    except Exception as e:
                        count_erros += 1
                        log.debug(f"Erro BD ({ev.titulo}): {e}")
                
                await self.session.commit()
                relatorio[nome] = {"capturados": len(eventos), "persistidos": count_persistidos, "erros": count_erros}
                
                log.info(f"[Manager] {nome}: {len(eventos)} capturados | {count_persistidos} novos | {count_erros} erros")
                todos_eventos.extend(eventos)
                
            except Exception as e:
                log.error(f"❌ Falha no motor {nome}: {e}")
                await self.session.rollback()

        total_cap = sum(r.get("capturados", 0) for r in relatorio.values())
        total_pers = sum(r.get("persistidos", 0) for r in relatorio.values())
        log.info(f"✨ CICLO COMPLETO: {total_cap} capturados | {total_pers} novos no banco")
        return todos_eventos

DataManager = EventManager
FILE_EOF

# 2. PORTAL BH v4.0 (Drupal 7/8 JSON e Zero-Indexed Pages)
cat << 'FILE_EOF' > app/services/extractors/portal_bh_service.py
import hashlib, httpx, asyncio, re
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PortalBHExtractor(BaseExtractor):
    BASE_URL = "https://portalbelohorizonte.com.br"
    URLS = ["/eventos", "/agenda", "/lazer-e-turismo/agenda-cultural"]

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/html"}
        
        async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
            for path in self.URLS:
                for page in range(0, 3): # v4: Zero-indexed para Drupal
                    url = f"{self.BASE_URL}{path}?page={page}"
                    try:
                        # Prioridade 1: API JSON Nativa do Drupal
                        r_json = await client.get(f"{url}&_format=json")
                        if r_json.status_code == 200:
                            dados = r_json.json()
                            if isinstance(dados, list):
                                for item in dados:
                                    tit = re.sub(r'<[^>]+>', '', item.get("title", "")).strip()
                                    if len(tit) > 5:
                                        uid = hashlib.md5(f"{tit}{path}".encode()).hexdigest()
                                        eventos[uid] = EventoSchema(
                                            id_unico=uid, titulo=tit[:250], data_evento=datetime.now(),
                                            cidade="Belo Horizonte", local="Belo Horizonte",
                                            categoria="Cultura", preco_base=0.0,
                                            url_evento=f"{self.BASE_URL}{path}", fonte="Portal BH (JSON)"
                                        )
                        
                        # Prioridade 2: Drupal Views (HTML)
                        r_html = await client.get(url)
                        if r_html.status_code == 200:
                            tree = HTMLParser(r_html.text)
                            for node in tree.css("div.views-row, div.view-content .views-row"):
                                a_node = node.css_first("a")
                                if a_node and len(a_node.text(strip=True)) > 5:
                                    tit = a_node.text(strip=True)[:250]
                                    uid = hashlib.md5(tit.encode()).hexdigest()
                                    eventos[uid] = EventoSchema(
                                        id_unico=uid, titulo=tit, data_evento=datetime.now(),
                                        cidade="Belo Horizonte", local="Belo Horizonte",
                                        categoria="Cultura", preco_base=0.0,
                                        url_evento=self.BASE_URL, fonte="Portal BH (HTML)"
                                    )
                        await asyncio.sleep(0.5)
                    except httpx.RequestError as e:
                        log.debug(f"[PortalBH] Falha na rede {url}: {e}")
                        break
        return list(eventos.values())
FILE_EOF

# 3. SYMPLA v4.0 (API Interna React SPA)
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
import hashlib, httpx, asyncio
from datetime import datetime
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class SymplaExtractor(BaseExtractor):
    SEARCH_API = "https://www.sympla.com.br/api/v1/search"

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        async with httpx.AsyncClient(timeout=25.0, headers=headers) as client:
            for page in range(1, 4):
                params = {"city": "belo-horizonte-mg", "page": page, "only": "events"}
                try:
                    resp = await client.get(self.SEARCH_API, params=params)
                    if resp.status_code != 200:
                        break
                    
                    data = resp.json()
                    itens = data.get("data", [])
                    if not itens:
                        break
                        
                    for ev in itens:
                        titulo = ev.get("name", "").strip()[:250]
                        url_ev = ev.get("url", "")
                        imagem = ev.get("image", "")
                        loc = ev.get("location", {}).get("city", "Belo Horizonte")
                        uid = hashlib.md5(url_ev.encode()).hexdigest()
                        
                        eventos[uid] = EventoSchema(
                            id_unico=uid, titulo=titulo, data_evento=datetime.now(),
                            cidade="Belo Horizonte", local=loc, categoria="Sympla Eventos",
                            preco_base=0.0, url_evento=url_ev, imagem_url=imagem, fonte="Sympla (API)"
                        )
                    await asyncio.sleep(0.6)
                except Exception as e:
                    log.debug(f"[Sympla] Falha na API: {e}")
                    break
                    
        return list(eventos.values())
FILE_EOF

# 4. PALÁCIO DAS ARTES v4.0 (WordPress Real)
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
                        for node in tree.css("article.post, div.post"):
                            tit_node = node.css_first("h2.entry-title, h3, a")
                            if tit_node and len(tit_node.text(strip=True)) > 5:
                                tit = tit_node.text(strip=True)[:250]
                                uid = hashlib.md5(tit.encode()).hexdigest()
                                eventos[uid] = EventoSchema(
                                    id_unico=uid, titulo=tit, data_evento=datetime.now(),
                                    cidade="Belo Horizonte", local="Palácio das Artes",
                                    categoria="FCS/Cultura", preco_base=0.0, url_evento=url,
                                    fonte="FCS / Palácio das Artes"
                                )
                    await asyncio.sleep(0.5)
                except httpx.RequestError as e:
                    log.debug(f"[Palácio Artes] Falha em {url}: {e}")
                    
        return list(eventos.values())
FILE_EOF

# 5. DIÁRIO AMM v4.0 (Full-Text Search)
cat << 'FILE_EOF' > app/services/extractors/diario_amm_service.py
import hashlib, httpx, asyncio
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class DiarioAMMExtractor(BaseExtractor):
    BUSCA_URL = "https://www.diariomunicipal.com.br/amm-mg/pesquisar"
    QUERIES = ["show musical", "apresentacao artistica", "festival"]

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        
        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            for query in self.QUERIES:
                try:
                    resp = await client.get(f"{self.BUSCA_URL}?q={query}")
                    if resp.status_code == 200:
                        tree = HTMLParser(resp.text)
                        for node in tree.css("div.box-resultados article"):
                            tit_node = node.css_first("h3")
                            if tit_node and len(tit_node.text(strip=True)) > 5:
                                tit = tit_node.text(strip=True)[:250]
                                uid = hashlib.md5(tit.encode()).hexdigest()
                                eventos[uid] = EventoSchema(
                                    id_unico=uid, titulo=tit, data_evento=datetime.now(),
                                    cidade="Interior MG", local="Diário Oficial",
                                    categoria="Licitação Show", preco_base=0.0,
                                    url_evento=self.BUSCA_URL, fonte="Diário AMM"
                                )
                    await asyncio.sleep(1.0)
                except httpx.RequestError as e:
                    log.debug(f"[DiarioAMM] Erro de rede na query '{query}': {e}")
                    
        return list(eventos.values())
FILE_EOF

echo "✅ Ambiente v15.2.0 reconstruído com fidelidade. Rode 'python inaugurar_sistema.py'."
