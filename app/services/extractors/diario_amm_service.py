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
