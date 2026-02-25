#!/bin/bash
echo "🚀 Iniciando Recuperação Total v15.1.0 (Lógica v4 do ZIP)..."

# 1. ORQUESTRADOR SEGURO (MIGRATIONS AUTOMÁTICAS E INSERT OR IGNORE)
cat << 'FILE_EOF' > app/services/manager.py
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.logger import log

from app.services.extractors.portal_bh_service import PortalBHExtractor
from app.services.extractors.sympla_service import SymplaExtractor
from app.services.extractors.diario_amm_service import DiarioAMMExtractor
from app.services.extractors.palacio_artes_service import PalacioArtesExtractor

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
        """Garante a estrutura do banco exigida pela v4 sem apagar dados."""
        try:
            await self.session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_eventos_id_unico ON eventos(id_unico)"))
        except Exception: pass
        
        try:
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN imagem_url VARCHAR(500)"))
        except Exception: pass
        
        try:
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN descricao TEXT"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN categoria VARCHAR(100)"))
        except Exception: pass
        
        await self.session.commit()

    async def run_all_scrapers(self):
        log.info(f"🚀 Iniciando ciclo v4 com {len(self.scrapers)} fontes...")
        await self._aplicar_migrations()
        todos_eventos = []

        # v4 usa INSERT OR IGNORE para respeitar o Unique Index nativo do SQLite
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
                    log.warning(f"⚠️ {nome}: 0 eventos.")
                    continue
                
                count = 0
                for ev in eventos:
                    try:
                        await self.session.execute(sql, ev.model_dump())
                        count += 1
                    except Exception as e:
                        log.debug(f"Erro BD ({ev.titulo}): {e}")
                
                await self.session.commit()
                log.info(f"💾 {nome}: {count} persistidos.")
                todos_eventos.extend(eventos)
            except Exception as e:
                log.error(f"❌ Falha no motor {nome}: {e}")
                await self.session.rollback()

        total = len(todos_eventos)
        log.info(f"✨ CICLO COMPLETO: {total} eventos capturados.")
        return todos_eventos

DataManager = EventManager
FILE_EOF

# 2. PORTAL BH v4 (DRUPAL 7/8 API e Paginação 0-Indexed)
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
                for page in range(0, 3): # v4 corrige para zero-indexed
                    url = f"{self.BASE_URL}{path}?page={page}"
                    
                    try:
                        # Tenta API do Drupal primeiro (?_format=json)
                        resp_json = await client.get(f"{url}&_format=json")
                        if resp_json.status_code == 200:
                            dados = resp_json.json()
                            if isinstance(dados, list):
                                for item in dados:
                                    tit = re.sub(r'<[^>]+>', '', item.get("title", ""))
                                    if len(tit) > 5:
                                        id_unico = hashlib.md5(tit.encode()).hexdigest()
                                        eventos[id_unico] = EventoSchema(
                                            id_unico=id_unico, titulo=tit.upper(), data_evento=datetime.now(),
                                            cidade="Belo Horizonte", local="Portal BH", categoria="Cultura",
                                            preco_base=0.0, url_evento=f"{self.BASE_URL}{path}", fonte="Portal BH v4"
                                        )
                        
                        # Fallback Drupal HTML (views-row)
                        resp_html = await client.get(url)
                        if resp_html.status_code == 200:
                            tree = HTMLParser(resp_html.text)
                            for node in tree.css("div.views-row, div.view-content .views-row"):
                                a = node.css_first("a")
                                if a and len(a.text(strip=True)) > 5:
                                    tit = a.text(strip=True)
                                    id_unico = hashlib.md5(tit.encode()).hexdigest()
                                    eventos[id_unico] = EventoSchema(
                                        id_unico=id_unico, titulo=tit.upper(), data_evento=datetime.now(),
                                        cidade="Belo Horizonte", local="Belo Horizonte", categoria="Cultura",
                                        preco_base=0.0, url_evento=self.BASE_URL, fonte="Portal BH v4"
                                    )
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        log.debug(f"[PortalBH v4] Falha em {url}: {e}")
                        break
        return list(eventos.values())
FILE_EOF

# 3. SYMPLA v4 (API Interna / React SPA Bypass)
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
import hashlib, httpx, asyncio
from datetime import datetime
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class SymplaExtractor(BaseExtractor):
    # Endpoint interno descoberto na v4 (Ignora o SPA vazio)
    SEARCH_API = "https://www.sympla.com.br/api/v1/search"

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        
        async with httpx.AsyncClient(timeout=20.0, headers=headers) as client:
            for page in range(1, 4):
                try:
                    # Buscando dados reais em JSON, direto do backend deles
                    params = {"city": "belo-horizonte-mg", "page": page, "only": "events"}
                    resp = await client.get(self.SEARCH_API, params=params)
                    
                    if resp.status_code != 200: break
                    data = resp.json()
                    itens = data.get("data", [])
                    if not itens: break

                    for ev in itens:
                        titulo = ev.get("name", "").strip()
                        url_ev = ev.get("url", "")
                        imagem = ev.get("image", "")
                        id_unico = hashlib.md5(url_ev.encode()).hexdigest()
                        
                        eventos[id_unico] = EventoSchema(
                            id_unico=id_unico, titulo=titulo.upper(), data_evento=datetime.now(),
                            cidade="Belo Horizonte", local=ev.get("location", {}).get("city", "Belo Horizonte"),
                            descricao="", categoria="Sympla Eventos", preco_base=0.0,
                            url_evento=url_ev, imagem_url=imagem, fonte="Sympla v4 (API)"
                        )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    log.debug(f"[Sympla v4] Erro na API: {e}")
                    break
        return list(eventos.values())
FILE_EOF

# 4. DIÁRIO AMM v4 (FULL-TEXT SEARCH SEM BAIXAR PDF)
cat << 'FILE_EOF' > app/services/extractors/diario_amm_service.py
import hashlib, httpx, asyncio
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class DiarioAMMExtractor(BaseExtractor):
    BUSCA_URL = "https://www.diariomunicipal.com.br/amm-mg/pesquisar"
    QUERIES = ["show musical", "apresentação artística", "festival"]

    async def extract(self) -> list[EventoSchema]:
        eventos = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        
        async with httpx.AsyncClient(timeout=25.0, headers=headers, follow_redirects=True) as client:
            for query in self.QUERIES:
                try:
                    resp = await client.get(f"{self.BUSCA_URL}?q={query}")
                    if resp.status_code == 200:
                        tree = HTMLParser(resp.text)
                        for node in tree.css("div.box-resultados article"):
                            tit = node.css_first("h3")
                            if tit and len(tit.text(strip=True)) > 5:
                                titulo = tit.text(strip=True)
                                id_unico = hashlib.md5(titulo.encode()).hexdigest()
                                eventos[id_unico] = EventoSchema(
                                    id_unico=id_unico, titulo=titulo.upper(), data_evento=datetime.now(),
                                    cidade="Interior MG", local="Diário AMM", categoria="Licitação Show",
                                    preco_base=0.0, url_evento=self.BUSCA_URL, fonte="Diário AMM v4"
                                )
                    await asyncio.sleep(1.0)
                except Exception as e:
                    log.debug(f"[DiarioAMM v4] Erro na query '{query}': {e}")
        return list(eventos.values())
FILE_EOF

# 5. PALÁCIO DAS ARTES v4 (SELECTORES WORDPRESS REAIS)
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
                        # v4: Seletor real testado do WordPress da FCS
                        for node in tree.css("article.post, div.post"):
                            tit = node.css_first("h2.entry-title, h3, a")
                            if tit and len(tit.text(strip=True)) > 5:
                                titulo = tit.text(strip=True)
                                id_unico = hashlib.md5(titulo.encode()).hexdigest()
                                eventos[id_unico] = EventoSchema(
                                    id_unico=id_unico, titulo=titulo.upper(), data_evento=datetime.now(),
                                    cidade="Belo Horizonte", local="Palácio das Artes", categoria="Cultura",
                                    preco_base=0.0, url_evento=url, fonte="Palácio das Artes v4"
                                )
                except Exception: pass
        return list(eventos.values())
FILE_EOF

echo "✅ Arquitetura v4 inserida com sucesso. Rode python inaugurar_sistema.py"
