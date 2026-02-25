"""
Extrator Portal O TEMPO v1.0
Foco: Divulgação de eventos e entretenimento em MG.
Substitui o G1 devido ao bloqueio de IP/Headers.
"""
import httpx
from selectolax.parser import HTMLParser
from datetime import datetime, timedelta
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class OTempoExtractor(BaseExtractor):
    URL = "https://www.otempo.com.br/entretenimento"

    async def extract(self) -> list[EventoSchema]:
        eventos = []
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                resp = await client.get(self.URL)
                if resp.status_code != 200:
                    log.error(f"⚠️ O TEMPO recusou: {resp.status_code}")
                    return []

                tree = HTMLParser(resp.text)
                # Seleciona as chamadas de matérias de entretenimento
                for card in tree.css("a[href*='/entretenimento/']"):
                    titulo = card.text(strip=True)
                    url = card.attributes.get("href")
                    
                    if len(titulo) > 20: # Filtra links de menu
                        eventos.append(EventoSchema(
                            titulo=f"DIVULGAÇÃO: {titulo.upper()}",
                            data_evento=datetime.now() + timedelta(days=3),
                            cidade="Minas Gerais",
                            local="Ver no Portal O Tempo",
                            preco_base=0.0,
                            fonte="Portal O Tempo",
                            url_origem=url if url.startswith("http") else f"https://www.otempo.com.br{url}",
                            vibe="show"
                        ))
        except Exception as e:
            log.error(f"❌ Erro no O TEMPO: {e}")
        return eventos
