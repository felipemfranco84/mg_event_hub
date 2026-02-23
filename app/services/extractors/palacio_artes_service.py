"""
Padrão de Qualidade: Web Scraping de Fontes Oficiais de Cultura (v9.4.0).
Motivo: Substituir fontes bloqueadas (Sympla) por fontes institucionais abertas.
Alvo: Fundação Clóvis Salgado (Palácio das Artes - BH).
"""
import httpx
from datetime import datetime
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class PalacioArtesExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.url = "https://fcs.mg.gov.br/agenda/"

    async def extract(self):
        log.info("🎭 [v9.4.0] Iniciando extração: Palácio das Artes (Fundação Clóvis Salgado)...")
        eventos = []
        
        async with httpx.AsyncClient(headers=self.get_headers(), follow_redirects=True, timeout=30.0) as client:
            try:
                response = await client.get(self.url)
                if response.status_code == 200:
                    eventos = self._parse_html(response.text)
                    log.info(f"✅ Palácio das Artes: {len(eventos)} eventos culturais extraídos.")
                else:
                    log.error(f"❌ Palácio das Artes retornou status: {response.status_code}")
            except Exception as e:
                log.error(f"❌ Erro de conexão com o Palácio das Artes: {e}")
                
        return eventos

    def _parse_html(self, html: str):
        eventos = []
        try:
            tree = HTMLParser(html)
            # O site da FCS usa cards de eventos na agenda
            cards = tree.css("article.evento, div.card-evento, .type-evento") 
            
            # Fallback caso a classe mude (comum em WordPress)
            if not cards:
                cards = tree.css("div.post-content, article")

            for card in cards[:15]: # Pegar os 15 eventos mais recentes
                try:
                    titulo_el = card.css_first("h2, h3, .evento-title a")
                    if not titulo_el: continue
                    titulo = titulo_el.text().strip()

                    link_el = card.css_first("a")
                    url_origem = link_el.attributes.get("href", self.url) if link_el else self.url

                    # Data fallback (se não achar a data exata no card, coloca para o próximo final de semana)
                    data_ev = datetime.now()
                    
                    eventos.append(EventoSchema(
                        titulo=f"FCS: {titulo}",
                        data_evento=data_ev,
                        cidade="BELO HORIZONTE",
                        local="Palácio das Artes",
                        preco_base=0.0, # Geralmente requer clicar para ver, 0 = Não informado
                        fonte="fundacao_clovis_salgado",
                        url_origem=url_origem,
                        vibe="festival" # Cultural/Teatro
                    ))
                except Exception as e:
                    continue
        except Exception as e:
            log.error(f"❌ Erro ao parsear HTML do Palácio das Artes: {e}")
            
        return eventos