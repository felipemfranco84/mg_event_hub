#!/bin/bash
echo "🚀 A aplicar correcção de DOM Profundo (v15.7.1)..."

# ==============================================================================
# 1. SYMPLA SERVICE (DOM Profundo)
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
"""
SymplaExtractor v15.7.1 — Motor de Extracção via HTML Fallback
══════════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
A API interna falha com 404. O scraping agora utiliza um mergulho profundo
no DOM (Deep Parsing). Em vez de extrair todo o texto da tag <a> (o que agrupa
preço, data e título numa única string e falha os filtros), procuramos as
tags de cabeçalho (h3, h2) ou divs de título dentro do card do evento.
"""
import hashlib
import httpx
import asyncio
from datetime import datetime
from selectolax.parser import HTMLParser

from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class SymplaExtractor(BaseExtractor):
    URL_ALVO = "https://www.sympla.com.br/eventos/belo-horizonte-mg"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9"
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Sympla] A requisitar HTML: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Sympla] Falha na rede: HTTP {resp.status_code}")
                    return []

                tree = HTMLParser(resp.text)
                nodes_evento = tree.css("a[href*='/evento/']")
                
                for a_node in nodes_evento:
                    href = a_node.attributes.get("href", "")
                    
                    # Mergulho Profundo: Procura o título isolado dentro do link
                    titulo_node = a_node.css_first("h3, h2, h4, div[class*='EventTitle'], div[class*='title']")
                    
                    # Se achar o nó específico, extrai limpo. Se não, tenta fallback cauteloso.
                    if titulo_node:
                        titulo_bruto = titulo_node.text(strip=True)
                    else:
                        titulo_bruto = a_node.text(strip=True)
                    
                    if len(titulo_bruto) < 5 or "Sympla" in titulo_bruto:
                        continue
                        
                    titulo = titulo_bruto[:250].replace('\n', ' ').strip()
                    url_completa = href if href.startswith("http") else f"https://www.sympla.com.br{href}"
                    uid = hashlib.md5(url_completa.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo,
                            data_evento=datetime.now(),
                            cidade="Belo Horizonte",
                            local="Sympla",
                            categoria="Entretenimento",
                            preco_base=0.0,
                            url_evento=url_completa,
                            fonte="Sympla (HTML Rescue)"
                        )
                        
        except Exception as e:
            log.error(f"[Sympla] Falha no processamento: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

# ==============================================================================
# 2. PALÁCIO DAS ARTES SERVICE (DOM Profundo)
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/palacio_artes_service.py
"""
PalacioArtesExtractor v15.7.1 — Motor WordPress Adaptado
════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
Separa a detecção do contentor (div.evento, article) da detecção do texto.
Mergulha no contentor à procura estrita de h2.entry-title ou h3, impedindo
que o texto de botões de 'Comprar Ingresso' se misturem ao título.
"""
import hashlib
import httpx
import asyncio
from datetime import datetime
from selectolax.parser import HTMLParser

from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PalacioArtesExtractor(BaseExtractor):
    URL_ALVO = "https://fcs.mg.gov.br/programacao/"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        
        try:
            async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Palácio das Artes] GET HTML: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Palácio] Código: {resp.status_code}")
                    return []
                    
                tree = HTMLParser(resp.text)
                contentores = tree.css("div.evento, article")
                
                for card in contentores:
                    # Mergulho Profundo: Isola a busca do título da busca do link
                    titulo_node = card.css_first("h2.entry-title, h3, h2.elementor-heading-title a, h2")
                    if not titulo_node: 
                        continue
                    
                    titulo = titulo_node.text(strip=True)
                    if len(titulo) < 5: 
                        continue
                    
                    # Busca o link separadamente
                    a_node = card.css_first("a")
                    href = a_node.attributes.get("href", self.URL_ALVO) if a_node else self.URL_ALVO
                    
                    uid = hashlib.md5(titulo.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo.upper()[:250],
                            data_evento=datetime.now(),
                            cidade="Belo Horizonte",
                            local="FCS / Palácio das Artes",
                            categoria="Artes Cénicas / Música",
                            preco_base=0.0,
                            url_evento=href,
                            fonte="FCS (WordPress)"
                        )
                        
        except Exception as e:
            log.error(f"[Palácio] Erro de extracção: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

echo "✅ Ficheiros actualizados com Deep Parsing."
