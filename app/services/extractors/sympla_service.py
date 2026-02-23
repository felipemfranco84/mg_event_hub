"""
Padrão de Qualidade: Schema.org JSON-LD Extraction (v9.3.2).
Motivo: Como o Next.js State foi ocultado pelo Sympla, utilizamos a tag padrão 
de SEO (JSON-LD) que todo site de eventos precisa manter visível no HTML.
"""
import json
import httpx
from datetime import datetime
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class SymplaExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.cidades_mg = ["belo-horizonte", "uberlandia", "juiz-de-fora", "ouropreto"]

    async def extract(self):
        log.info("🚀 [v9.3.2] Iniciando Sympla Extractor via JSON-LD SEO...")
        todos_eventos = []
        
        async with httpx.AsyncClient(headers=self.get_headers(), follow_redirects=True, timeout=30.0) as client:
            for cidade in self.cidades_mg:
                try:
                    url = f"https://www.sympla.com.br/eventos/{cidade}"
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        eventos = self._parse_json_ld(response.text, cidade)
                        todos_eventos.extend(eventos)
                        log.info(f"✅ Sympla: {len(eventos)} eventos extraídos em {cidade}.")
                except Exception as e:
                    log.error(f"❌ Erro de rede ao acessar Sympla ({cidade}): {e}")
                    
        return todos_eventos

    def _parse_json_ld(self, html: str, cidade: str):
        eventos = []
        try:
            tree = HTMLParser(html)
            # Busca todas as tags de dados estruturados
            script_tags = tree.css("script[type='application/ld+json']")
            
            if not script_tags:
                log.warning(f"⚠️ Nenhuma tag JSON-LD encontrada em {cidade}.")
                return []

            for script in script_tags:
                try:
                    dados = json.loads(script.text())
                    
                    # O JSON-LD pode ser uma lista ou um objeto único
                    lista_dados = dados if isinstance(dados, list) else [dados]
                    
                    for item in lista_dados:
                        # Verifica se é do tipo Evento (Schema.org)
                        if item.get("@type") == "Event":
                            titulo = item.get("name", "Evento Sympla")
                            url_ev = item.get("url", f"https://www.sympla.com.br/eventos/{cidade}")
                            
                            # Parse da Data (ISO Format)
                            data_str = item.get("startDate")
                            data_ev = datetime.now()
                            if data_str:
                                clean_date = data_str.split("T")[0]
                                data_ev = datetime.strptime(clean_date, "%Y-%m-%d")
                            
                            # Parse do Local
                            local_obj = item.get("location", {})
                            local_nome = local_obj.get("name", "Local a confirmar")
                            
                            # Parse do Preço (Se houver oferta)
                            preco = 0.0
                            offers = item.get("offers")
                            if isinstance(offers, list) and len(offers) > 0:
                                preco = float(offers[0].get("price", 0.0))
                            elif isinstance(offers, dict):
                                preco = float(offers.get("price", 0.0))

                            eventos.append(EventoSchema(
                                titulo=titulo,
                                data_evento=data_ev,
                                cidade=cidade.replace("-", " ").title(),
                                local=local_nome,
                                preco_base=preco,
                                fonte="sympla_seo",
                                url_origem=url_ev,
                                vibe="festival" if "festival" in titulo.lower() else "show"
                            ))
                except Exception as e:
                    continue # Pula script inválido e tenta o próximo
                    
        except Exception as e:
            log.error(f"❌ Falha global no parser do Sympla: {e}")
            
        return eventos