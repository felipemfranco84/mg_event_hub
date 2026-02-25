import hashlib
import httpx
import re
from datetime import datetime, timedelta
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PortalBHExtractor(BaseExtractor):
    BASE_URL = "https://portalbelohorizonte.com.br"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0"}
        async with httpx.AsyncClient(timeout=25.0, headers=headers) as client:
            resp = await client.get(f"{self.BASE_URL}/eventos")
            tree = HTMLParser(resp.text)
            
            for card in tree.css(".views-row, article"):
                a_node = card.css_first("a[href*='/eventos/']")
                if not a_node: continue
                
                titulo = a_node.text(strip=True)
                url = self.BASE_URL + a_node.attributes.get("href", "")
                
                # --- TÉCNICA DE EXTRAÇÃO DE DATA ---
                # Procura por padrões dd/mm ou classes de data
                texto_card = card.text().lower()
                data_obj = datetime.now() + timedelta(days=2) # Default: daqui a 2 dias (Evita 'Hoje')
                
                match = re.search(r'(\d{2})[/\-](\d{2})', texto_card)
                if match:
                    dia, mes = map(int, match.groups())
                    data_obj = datetime(2026, mes, dia, 19, 0) # Força ano 2026

                uid = hashlib.md5(url.encode()).hexdigest()
                if uid not in eventos_unicos:
                    eventos_unicos[uid] = EventoSchema(
                        id_unico=uid,
                        titulo=titulo,
                        data_evento=data_obj,
                        cidade="Belo Horizonte",
                        local="Portal BH",
                        fonte="Portal BH"
                    )
        return list(eventos_unicos.values())
