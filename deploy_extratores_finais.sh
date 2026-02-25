#!/bin/bash
echo "🚀 A iniciar o deploy dos extractores (v15.7.0)..."

# ==============================================================================
# 1. SYMPLA SERVICE
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
"""
SymplaExtractor v15.7.0 — Motor de Extracção via HTML Fallback
══════════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
A API interna (/api/v1/search) foi descontinuada (retorna 404). 
Para contornar a ausência de um endpoint JSON público e evitar o SPA vazio,
esta lógica faz a requisição do HTML renderizado pelo servidor e aplica
um fallback de segurança, extraindo directamente as tags <a> que contêm links 
para eventos.
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9"
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Sympla] A iniciar requisição à URL: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Sympla] Falha na requisição. Código HTTP: {resp.status_code}")
                    return []

                tree = HTMLParser(resp.text)
                
                # Procura todas as hiperligações que apontem para a página de um evento
                nodes_evento = tree.css("a[href*='/evento/']")
                log.debug(f"[Sympla] Encontrados {len(nodes_evento)} nós potenciais de eventos.")
                
                for a_node in nodes_evento:
                    href = a_node.attributes.get("href", "")
                    titulo_bruto = a_node.text(strip=True)
                    
                    # Filtra lixo e menus de navegação
                    if len(titulo_bruto) < 10 or "Sympla" in titulo_bruto:
                        continue
                        
                    titulo = titulo_bruto[:250].replace('\n', ' ').strip()
                    url_completa = href if href.startswith("http") else f"https://www.sympla.com.br{href}"
                    
                    # Deduplicação por hash da URL
                    uid = hashlib.md5(url_completa.encode('utf-8')).hexdigest()
                    
                    if uid not in eventos_unicos:
                        eventos_unicos[uid] = EventoSchema(
                            id_unico=uid,
                            titulo=titulo,
                            data_evento=datetime.now(), # Fallback se a data não estiver explícita no card
                            cidade="Belo Horizonte",
                            local="Sympla",
                            categoria="Entretenimento",
                            preco_base=0.0,
                            url_evento=url_completa,
                            fonte="Sympla (HTML Rescue)"
                        )
                        
        except httpx.RequestError as e:
            log.error(f"[Sympla] Erro crítico de rede: {e}")
        except Exception as e:
            log.error(f"[Sympla] Erro inesperado durante o parsing: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

# ==============================================================================
# 2. PORTAL BH SERVICE
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/portal_bh_service.py
"""
PortalBHExtractor v15.7.0 — Motor Drupal Zero-Indexed
══════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
A API do Drupal (?_format=json) está protegida por um WAF que devolve 406 Not Acceptable.
A lógica actual varre o HTML paginado. Nota técnica: a paginação do Drupal começa no índice 0.
Extraímos links genéricos ('/eventos/') pois os selectores de classes alteram-se com frequência.
"""
import hashlib
import httpx
import asyncio
from datetime import datetime
from selectolax.parser import HTMLParser

from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PortalBHExtractor(BaseExtractor):
    BASE_URL = "https://portalbelohorizonte.com.br"
    CAMINHOS = ["/eventos", "/agenda"]

    async def extract(self) -> list[EventoSchema]:
        eventos_unicos = {}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        async with httpx.AsyncClient(timeout=25.0, headers=headers, follow_redirects=True) as client:
            for caminho in self.CAMINHOS:
                for pagina in range(0, 3): # Drupal 7/8 usa page=0 para a primeira página
                    url_alvo = f"{self.BASE_URL}{caminho}" + (f"?page={pagina}" if pagina > 0 else "")
                    
                    try:
                        log.debug(f"[Portal BH] A aceder à página: {url_alvo}")
                        resp = await client.get(url_alvo)
                        
                        if resp.status_code != 200:
                            log.warning(f"[Portal BH] Fim da paginação ou bloqueio (HTTP {resp.status_code}) em {url_alvo}")
                            break
                        
                        tree = HTMLParser(resp.text)
                        
                        # Fallback robusto: vasculha qualquer tag 'a' que leve a um evento
                        for a_node in tree.css("a"):
                            href = a_node.attributes.get("href", "")
                            if not href or ("/eventos/" not in href and "/agenda/" not in href):
                                continue
                                
                            titulo = a_node.text(strip=True)
                            
                            # Limpeza de ruído comum de interface
                            if len(titulo) < 10 or "Leia mais" in titulo or "whatsapp" in titulo.lower():
                                continue
                                
                            url_completa = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                            uid = hashlib.md5(url_completa.encode('utf-8')).hexdigest()
                            
                            if uid not in eventos_unicos:
                                eventos_unicos[uid] = EventoSchema(
                                    id_unico=uid,
                                    titulo=titulo[:250],
                                    data_evento=datetime.now(),
                                    cidade="Belo Horizonte",
                                    local="Portal BH",
                                    categoria="Cultura Institucional",
                                    preco_base=0.0,
                                    url_evento=url_completa,
                                    fonte="Portal BH (Drupal HTML)"
                                )
                                
                        await asyncio.sleep(0.5) # Previne rate limit
                        
                    except httpx.RequestError as e:
                        log.error(f"[Portal BH] Falha de comunicação na URL {url_alvo}: {e}")
                        break
                    except Exception as e:
                        log.error(f"[Portal BH] Falha de processamento do DOM: {e}")
                        break
                        
        return list(eventos_unicos.values())
FILE_EOF

# ==============================================================================
# 3. PALÁCIO DAS ARTES SERVICE
# ==============================================================================
cat << 'FILE_EOF' > app/services/extractors/palacio_artes_service.py
"""
PalacioArtesExtractor v15.7.0 — Motor WordPress Adaptado
════════════════════════════════════════════════════════
MOTIVO DA LÓGICA:
O scanner forense confirmou que a estrutura do DOM do WordPress da FCS foi alterada.
As tags que antes funcionavam agora falham. A lógica actual vasculha por 'div.evento'
e 'article', que foram validadas como os contentores actuais dos dados.
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
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                log.debug(f"[Palácio das Artes] A varrer a programação em: {self.URL_ALVO}")
                resp = await client.get(self.URL_ALVO)
                
                if resp.status_code != 200:
                    log.error(f"[Palácio das Artes] O servidor recusou a ligação. Código: {resp.status_code}")
                    return []
                    
                tree = HTMLParser(resp.text)
                
                # A pesquisar nos contentores identificados no Raio-X
                contentores = tree.css("div.evento, article")
                log.debug(f"[Palácio das Artes] Mapeados {len(contentores)} contentores de artigo.")
                
                for card in contentores:
                    a_node = card.css_first("a")
                    if not a_node: 
                        continue
                    
                    titulo = a_node.text(strip=True)
                    if len(titulo) < 10: 
                        continue
                    
                    href = a_node.attributes.get("href", self.URL_ALVO)
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
                        
        except httpx.RequestError as e:
            log.error(f"[Palácio das Artes] Erro de rede ao tentar capturar o portal FCS: {e}")
        except Exception as e:
            log.error(f"[Palácio das Artes] Crash não previsto durante extracção: {e}")
            
        return list(eventos_unicos.values())
FILE_EOF

echo "✅ Deploy concluído. Ficheiros gerados correctamente na pasta app/services/extractors/."
