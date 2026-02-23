"""
Padrão de Qualidade: Advanced PDF Mining & Data Cleaning.
Motivo: Higienização de dados extraídos para remover termos jurídicos e ruídos de cabeçalho.
"""
import re
import io
import httpx
from pypdf import PdfReader
from datetime import datetime, timedelta
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class DiarioOficialExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.diariomunicipal.com.br/amm-mg/"
        self.re_data = re.compile(r"(\d{2}/\d{2}/\d{4})")
        self.re_valor = re.compile(r"R\$\s?(\d{1,3}(?:\.\d{3})*,\d{2})")
        # Termos para remover dos nomes das cidades
        self.lixo_juridico = [
            "AVISO DE", "LICITAÇÃO", "EXTRATO", "CONTRATO", "PROCESSO", 
            "ESTADO DE", "MINAS GERAIS", "DIÁRIO", "MUNICIPAL", "–", "-"
        ]

    async def extract(self):
        log.info("🚀 Iniciando Mineração Refinada de PDFs na AMM-MG")
        html = await self.fetch_html(self.base_url)
        if not html: return []

        tree = HTMLParser(html)
        pdf_input = tree.css_first("input#urlPdf")
        if not pdf_input: return []

        pdf_url = pdf_input.attributes.get("value")
        log.info(f"📄 PDF: {pdf_url[:60]}...")

        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            try:
                resp = await client.get(pdf_url)
                resp.raise_for_status()
                
                pdf_file = io.BytesIO(resp.content)
                reader = PdfReader(pdf_file)
                eventos_validados = []

                for i, page in enumerate(reader.pages):
                    texto = page.extract_text()
                    # Filtro de relevância por página
                    if any(k in texto.lower() for k in ["contratação", "show", "artista", "banda"]):
                        eventos_validados.extend(self._processar_texto_edital(texto, pdf_url))
                
                return eventos_validados
            except Exception as e:
                log.error(f"❌ Erro no processamento de PDF: {e}")
                return []

    def _processar_texto_edital(self, texto, url):
        encontrados = []
        # Split inteligente ignorando case
        blocos = re.split(r"PREFEITURA MUNICIPAL DE", texto, flags=re.IGNORECASE)
        
        for bloco in blocos:
            bloco_lower = bloco.lower()
            # Só processa se tiver indícios reais de show e não for um rodapé/cabeçalho curto
            if any(k in bloco_lower for k in ["contratação", "show", "artista", "banda"]) and len(bloco) > 150:
                try:
                    valor_match = self.re_valor.search(bloco)
                    datas = self.re_data.findall(bloco)
                    
                    # Limpeza da Cidade
                    cidade_suja = bloco.strip().split('\n')[0].upper()
                    cidade_limpa = self._limpar_string(cidade_suja)
                    
                    if len(cidade_limpa) < 3 or "DIÁRIO" in cidade_limpa:
                        continue

                    evento_data = {
                        "titulo": f"Evento Municipal: {cidade_limpa.title()}",
                        "data_evento": self._get_future_date(datas),
                        "cidade": cidade_limpa,
                        "local": "Praça Pública / Centro",
                        "preco_base": float(valor_match.group(1).replace(".", "").replace(",", ".")) if valor_match else 0.0,
                        "fonte": "amm_mg_pdf",
                        "url_origem": url,
                        "vibe": "show"
                    }
                    encontrados.append(EventoSchema(**evento_data))
                except Exception:
                    continue
        return encontrados

    def _limpar_string(self, texto: str) -> str:
        """Remove termos jurídicos e ruídos de string."""
        temp = texto
        for termo in self.lixo_juridico:
            temp = temp.replace(termo, "")
        # Remove espaços duplos e pontuação sobrando nas pontas
        return re.sub(r'\s+', ' ', temp).strip(" .,-–")

    def _get_future_date(self, datas):
        for d in datas:
            try:
                dt = datetime.strptime(d, "%d/%m/%Y")
                if dt > datetime.now(): return dt
            except: continue
        return datetime.now() + timedelta(days=45)
