"""
Padrão de Qualidade: Performance e Robustez.
Motivo: Extração de eventos do Sympla para BH usando Selectolax.
"""
from app.services.extractors.base import BaseExtractor
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.core.logger import log
from datetime import datetime

class SymplaExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.sympla.com.br/eventos/belo-horizonte-mg"

    async def extract(self):
        log.info("Iniciando extração Sympla - Belo Horizonte")
        html = await self.fetch_html(self.base_url)
        
        if not html:
            return []

        tree = HTMLParser(html)
        eventos_validados = []

        # Seletores baseados na estrutura comum do Sympla (precisam de monitoramento)
        for card in tree.css("a[data-testid='event-card']"):
            try:
                titulo = card.css_first("h3").text().strip()
                link = card.attributes.get("href")
                
                # Exemplo simplificado de parsing de data (Sympla varia muito, aqui vai a lógica base)
                # Em um cenário real, precisaríamos de um parser de data mais complexo
                data_str = card.css_first("div[class*='EventDate']").text().strip()
                
                evento_data = {
                    "titulo": titulo,
                    "data_evento": datetime.now(), # Placeholder para lógica de parse de data
                    "cidade": "Belo Horizonte",
                    "local": "Ver no Sympla",
                    "preco_base": 0.0,
                    "fonte": "sympla",
                    "url_origem": link,
                    "vibe": "show"
                }
                
                # Validação via Pydantic (O Cérebro)
                evento_obj = EventoSchema(**evento_data)
                eventos_validados.append(evento_obj)
                
            except Exception as e:
                log.warning(f"Falha ao processar card do Sympla: {e}")
                continue

        log.info(f"Extração concluída. {len(eventos_validados)} eventos encontrados.")
        return eventos_validados
