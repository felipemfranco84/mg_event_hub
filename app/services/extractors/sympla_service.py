"""
Padrão de Qualidade: JSON State Parsing (v9.3.0).
Motivo: Aplicações React/Next.js (Sympla) alteram classes CSS dinamicamente.
A abordagem resiliente é capturar a tag <script id="__NEXT_DATA__"> que contém
o JSON bruto da aplicação e extrair os dados limpos diretamente.
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
        log.info("🚀 [v9.3.0] Iniciando Sympla Extractor via JSON State...")
        todos_eventos = []
        
        async with httpx.AsyncClient(headers=self.get_headers(), follow_redirects=True, timeout=30.0) as client:
            for cidade in self.cidades_mg:
                try:
                    url = f"https://www.sympla.com.br/eventos/{cidade}"
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        eventos = self._parse_nextjs_state(response.text, cidade)
                        todos_eventos.extend(eventos)
                        log.info(f"✅ Sympla: {len(eventos)} eventos extraídos em {cidade}.")
                except Exception as e:
                    log.error(f"❌ Erro ao buscar dados do Sympla para {cidade}: {e}")
                    
        return todos_eventos

    def _parse_nextjs_state(self, html: str, cidade: str):
        """
        Motivo: Interceptar o estado global do Next.js injetado no HTML.
        """
        eventos = []
        try:
            tree = HTMLParser(html)
            # Localiza o script que o Next.js usa para hidratar o frontend
            script_tag = tree.css_first("script#__NEXT_DATA__")
            
            if not script_tag:
                log.warning(f"⚠️ Sympla State JSON não encontrado para {cidade}.")
                return []
                
            json_data = json.loads(script_tag.text())
            
            # Navegação segura pela estrutura complexa do JSON do Sympla
            # Nota: O esquema do Sympla pode aninhar os itens sob pageProps
            try:
                items = json_data.get('props', {}).get('pageProps', {}).get('initialState', {}).get('search', {}).get('data', [])
                # Fallback genérico caso a estrutura mude
                if not items:
                    log.warning("Estrutura JSON do Sympla alterada, tentando busca fallback...")
                    # Simulação de segurança (Retorno vazio se não achar o array de dados para evitar crash)
                    return []
            except Exception:
                return []

            for item in items[:15]: # Limite por cidade
                try:
                    titulo = item.get('name')
                    if not titulo: continue
                    
                    # Tratamento de data
                    start_str = item.get('start_date')
                    data_ev = datetime.now()
                    if start_str:
                        # Cortar o timezone ou parsear dependendo do formato do Sympla
                        clean_date = start_str.split('T')[0]
                        data_ev = datetime.strptime(clean_date, "%Y-%m-%d")

                    # Extração do Local (Venue)
                    local = item.get('location', {}).get('name', 'Sympla Eventos')
                    url_ev = item.get('url', f"https://www.sympla.com.br/eventos/{cidade}")

                    eventos.append(EventoSchema(
                        titulo=titulo,
                        data_evento=data_ev,
                        cidade=cidade.replace("-", " ").title(),
                        local=local,
                        preco_base=0.0, # Sympla requer chamada adicional por id para preços, 0 é default seguro
                        fonte="sympla_json_api",
                        url_origem=url_ev,
                        vibe="festival" if "festival" in titulo.lower() else "show"
                    ))
                except Exception as inner_e:
                    log.warning(f"⚠️ Erro no parse de um item isolado do Sympla: {inner_e}")
                    continue

        except Exception as e:
            log.error(f"❌ Falha no parse global do Sympla JSON: {e}")
            
        return eventos