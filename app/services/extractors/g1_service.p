"""
Extrator de Divulgação G1 v1.0
Foco: Capturar eventos e sugestões culturais da editoria 'O que fazer' do G1 Minas.
"""
import httpx
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from datetime import datetime, timedelta

class G1Extractor(BaseExtractor):
    URL = "https://g1.globo.com/mg/minas-gerais/o-que-fazer-em-minas-gerais/"

    async def extract(self) -> list[EventoSchema]:
        eventos = []
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
                resp = await client.get(self.URL)
                tree = HTMLParser(resp.text)
                
                # Seleciona os posts do feed de notícias
                for item in tree.css(".feed-post-body"):
                    link_el = item.css_first(".feed-post-link")
                    if not link_el: continue
                    
                    titulo = link_el.text(strip=True)
                    url = link_el.attributes.get('href')
                    
                    # O G1 sugere eventos para o final de semana ou datas próximas
                    # Como o RSS não traz a data do evento no título, colocamos a data atual como referência
                    eventos.append(EventoSchema(
                        titulo=f"DIVULGAÇÃO: {titulo.upper()}",
                        data_evento=datetime.now() + timedelta(days=2),
                        cidade="Minas Gerais",
                        local="Ver detalhes na matéria",
                        preco_base=0.0,
                        fonte="G1 Minas - O que fazer",
                        url_origem=url,
                        vibe="show"
                    ))
        except Exception:
            pass
        return eventos
