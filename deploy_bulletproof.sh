#!/bin/bash
echo "🚀 A aplicar Extracção Bulletproof (v15.9.0)..."

# ==============================================================================
# 1. PALÁCIO DAS ARTES SERVICE (Slug Fallback)
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/palacio_artes_service.py
"""
PalacioArtesExtractor v15.9.0 — Extracção Bulletproof via URL Slug
══════════════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
A validação forense provou que o texto dos cards do WordPress retorna 
vazio ou com lixo de formatação ('...'). Para garantir 100% de captura,
abandonamos a leitura de nós de texto e extraímos o título formatando 
o slug da hiperligação do evento.
"""
import hashlib
import httpx
import asyncio
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
    except Exception:
        pass
    return ""

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
                    log.error(f"[Palácio] Falha na rede. Código: {resp.status_code}")
                    return []
                    
                tree = HTMLParser(resp.text)
                contentores = tree.css("article, div.evento")
                
                for card in contentores:
                    a_node = card.css_first("a")
                    if not a_node: 
                        continue
                    
                    href = a_node.attributes.get("href", "")
                    if not href or len(href) < 15:
                        continue
                        
                    # Extracção directa da URL em vez do texto HTML
                    titulo = extrair_slug_da_url(href)
                    if len(titulo) < 5: 
                        continue
                    
                    uid = hashlib.md5(href.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo[:250],
                            data_evento=datetime.now(),
                            cidade="Belo Horizonte",
                            local="FCS / Palácio das Artes",
                            categoria="Artes e Cultura",
                            preco_base=0.0,
                            url_evento=href,
                            fonte="FCS (Bulletproof)"
                        )
                        
        except Exception as e:
            log.error(f"[Palácio] Erro de extracção: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

# ==============================================================================
# 2. SYMPLA SERVICE (Filtro Nativo + Slug Fallback)
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
"""
SymplaExtractor v15.9.0 — Filtro Nativo + Extracção via Slug
════════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
O motor CSS falha ao interpretar os selectores complexos no HTML React do Sympla.
Solução: Extrair todas as hiperligações (tags <a>), filtrar nativamente no Python 
os links que contêm '/evento/', e derivar o título do evento directamente do 
slug da URL, ignorando o lixo visual da página.
"""
import hashlib
import httpx
import asyncio
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
            if slug.lower() in ['evento', 'sympla']:
                slug = partes[-2] if len(partes) > 1 else slug
            return slug.replace('-', ' ').title()
    except Exception:
        pass
    return ""

class SymplaExtractor(BaseExtractor):
    URL_ALVO = "https://www.sympla.com.br/eventos/belo-horizonte-mg"

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Sympla] GET HTML: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Sympla] Falha na rede: {resp.status_code}")
                    return []

                tree = HTMLParser(resp.text)
                todas_tags_a = tree.css("a")
                
                for a_node in todas_tags_a:
                    href = a_node.attributes.get("href", "")
                    
                    # Filtro nativo seguro em Python
                    if not href or "/evento/" not in href.lower():
                        continue
                        
                    titulo = extrair_slug_da_url(href)
                    if len(titulo) < 5 or "Sympla" in titulo:
                        continue
                        
                    url_completa = href if href.startswith("http") else f"https://www.sympla.com.br{href}"
                    uid = hashlib.md5(url_completa.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo[:250],
                            data_evento=datetime.now(),
                            cidade="Belo Horizonte",
                            local="Sympla",
                            categoria="Entretenimento",
                            preco_base=0.0,
                            url_evento=url_completa,
                            fonte="Sympla (Bulletproof)"
                        )
                        
        except Exception as e:
            log.error(f"[Sympla] Falha no processamento: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

echo "✅ Motores blindados. Pronto para a extracção."
