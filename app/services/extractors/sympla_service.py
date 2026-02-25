"""
SymplaExtractor v15.10.0 — Regex Master + Slug Extractor
════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
A estrutura Next.js do Sympla esconde os links dentro de blocos de scripts
e JSONs internos, tornando o parsing de HTML (DOM) cego. A abordagem final
descarta o HTMLParser e aplica Regex directamente no texto bruto da resposta 
HTTP para capturar qualquer string que corresponda a uma URL de evento.
"""
import hashlib
import httpx
import asyncio
import re
from datetime import datetime

from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

def extrair_slug(url: str) -> str:
    try:
        partes = [p for p in url.split('/') if p and not p.isdigit()]
        if partes:
            slug = partes[-1].replace('-', ' ').title()
            return slug
    except:
        pass
    return ""

class SymplaExtractor(BaseExtractor):
    URL_ALVO = "https://www.sympla.com.br/eventos/belo-horizonte-mg"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Sympla] GET Texto Bruto: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Sympla] Falha na rede: {resp.status_code}")
                    return []

                texto_bruto = resp.text
                
                # Regex 1: Captura URLs completas (https://www.sympla.com.br/evento/nome/123)
                padrao_absoluto = r'https://www\.sympla\.com\.br/evento/[a-zA-Z0-9\-]+/[0-9]+'
                links_absolutos = re.findall(padrao_absoluto, texto_bruto)
                
                # Regex 2: Captura caminhos relativos ocultos no JSON ("/evento/nome/123")
                padrao_relativo = r'"(/evento/[a-zA-Z0-9\-]+/[0-9]+)"'
                links_relativos = re.findall(padrao_relativo, texto_bruto)
                
                # Unifica e normaliza tudo
                todos_links = links_absolutos + [f"https://www.sympla.com.br{path}" for path in links_relativos]
                links_unicos = list(set(todos_links))
                
                log.debug(f"[Sympla] Regex encontrou {len(links_unicos)} URLs de eventos.")
                
                for url_ev in links_unicos:
                    titulo = extrair_slug(url_ev)
                    
                    if len(titulo) < 5 or titulo.lower() == "evento":
                        continue
                        
                    uid = hashlib.md5(url_ev.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo[:250],
                            data_evento=datetime.now(),
                            cidade="Belo Horizonte",
                            local="Belo Horizonte (Sympla)",
                            categoria="Entretenimento",
                            preco_base=0.0,
                            url_evento=url_ev,
                            fonte="Sympla (Regex Master)"
                        )
                        
        except Exception as e:
            log.error(f"[Sympla] Falha catastrófica no Regex: {e}")
            
        return list(eventos_unicos.values())
