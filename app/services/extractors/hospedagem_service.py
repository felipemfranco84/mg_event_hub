"""
Padrão de Qualidade: Hospitality Intelligence.
Motivo: Mapear ofertas de hospedagem nas cidades onde detectamos eventos.
"""
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from datetime import datetime, timedelta
import random

class HospedagemExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        # Cidades que mineramos com frequência no D.O.
        self.cidades_alvo = ["Tiradentes", "Ouro Preto", "Capitólio", "Diamantina", "São João Del Rei"]

    async def extract(self):
        log.info("🏠 Iniciando busca por ofertas de Hospedagem em MG")
        eventos_hospedagem = []

        # Para cada cidade, simulamos a busca de uma "Pousada em Destaque"
        # Em uma fase avançada, usaríamos um parceiro de API ou scraping de meta-busca
        for cidade in self.cidades_alvo:
            try:
                # Simulando a detecção de uma oferta (Mock inteligente)
                # O objetivo aqui é estruturar a entrada da 'vibe' pousada
                pousada_nome = f"Pousada Recanto de {cidade}"
                
                evento_data = {
                    "titulo": f"OFERTA: {pousada_nome}",
                    "data_evento": datetime.now() + timedelta(days=random.randint(7, 30)),
                    "cidade": cidade.upper(),
                    "local": "Centro Histórico",
                    "preco_base": float(random.randint(250, 600)), # Preço médio da diária
                    "fonte": "parceiro_hospedagem",
                    "url_origem": f"https://www.booking.com/searchresults.html?ss={cidade}",
                    "vibe": "pousada"
                }

                evento_obj = EventoSchema(**evento_data)
                eventos_hospedagem.append(evento_obj)
                log.info(f"🛌 Hospedagem encontrada: {pousada_nome} em {cidade}")

            except Exception as e:
                log.error(f"Erro ao processar hospedagem em {cidade}: {e}")
                continue

        return eventos_hospedagem
