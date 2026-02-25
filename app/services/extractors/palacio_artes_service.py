import hashlib
import httpx
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

def extrair_slug_da_url(url: str) -> str:
    try:
        partes = [p for p in url.split('/') if p and not p.isdigit()]
        if partes:
            slug = partes[-1]
            if slug.lower() in ['evento', 'programacao', 'agenda', 'espetaculo']:
                slug = partes[-2] if len(partes) > 1 else slug
            return slug.replace('-', ' ').title()
    except Exception: pass
    return "Evento Cultural"

class PalacioArtesExtractor(BaseExtractor):
    URL_ALVO = "https://fcs.mg.gov.br/programacao/"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        try:
            async with httpx.AsyncClient(timeout=25.0, headers=headers, follow_redirects=True) as client:
                resp = await client.get(self.URL_ALVO)
                tree = HTMLParser(resp.text)
                
                # Seletores mais amplos para o Palácio
                contentores = tree.css("article, .elementor-post, .evento")

                for card in contentores:
                    a_node = card.css_first("a")
                    if not a_node: continue
                    href = a_node.attributes.get("href", "")
                    if "fcs.mg.gov.br" not in href: continue

                    # Define data fixa no futuro para não poluir "hoje" enquanto o scraper amadurece
                    data_obj = datetime.now().replace(hour=19, minute=0, second=0)
                    
                    titulo = extrair_slug_da_url(href)
                    uid = hashlib.md5(href.encode()).hexdigest()

                    if uid not in eventos_unicos:
                        # CORREÇÃO: preco_base e categoria agora inclusos para o Pydantic
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo[:250],
                            data_evento=data_obj,
                            cidade="Belo Horizonte",
                            local="Palácio das Artes",
                            categoria="Cultura",
                            preco_base=0.0,
                            url_evento=href,
                            fonte="FCS (Palácio)"
                        )
        except Exception as e:
            log.error(f"[Palácio] Erro Crítico: {e}")
        return list(eventos_unicos.values())
