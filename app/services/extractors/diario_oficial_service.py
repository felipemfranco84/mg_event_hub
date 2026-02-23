"""
Padrão de Qualidade: NLP com Veto Absoluto e TQDM (v9.3.2).
Motivo: Evitar a 'diluição de score' implementando uma lista de Veto (Hard Stop).
Se o bloco falar de asfalto, é descartado instantaneamente.
"""
import re
import io
import fitz
import httpx
import gc
from tqdm import tqdm
from datetime import datetime, timedelta
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class DiarioOficialExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.diariomunicipal.com.br/amm-mg/"
        
        # Scoring Positivo
        self.KEYWORDS_MUSICAIS = {
            "inexigibilidade": 20,
            "contratação de artista": 30,
            "show artístico": 30,
            "apresentação musical": 30,
            "banda musical": 20,
            "dupla sertaneja": 20,
            "festival": 15,
            "palco": 10
        }

        # VETO ABSOLUTO (Hard Stop) - Se tiver isso, morre na hora.
        self.PALAVRAS_VETO = [
            "asfáltic", "recapeamento", "pavimentação", "saneamento", "esgoto", 
            "peças e acessórios", "pneus", "lubrificantes", "merenda", 
            "gêneros alimentícios", "medicamentos", "informática", "software",
            "manutenção preventiva", "manutenção corretiva", "brita"
        ]
        
        self.SCORE_THRESHOLD = 35 
        
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        # Regex mais estrita para o Artista
        self.re_artista = re.compile(r"(?:CONTRATADO:|CONTRATADA:|SHOW DA BANDA|APRESENTAÇÃO D(?:O|A|E)|ARTISTA:?)\s*([A-ZÀ-Ú0-9\s&\'\-]{4,40})(?:-|\.|,|CNPJ|CPF|PARA)", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")
        self.re_data = re.compile(r"(\d{2}/\d{2}/\d{4})")
        
        # O nome capturado NÃO pode conter essas palavras
        self.FILTRO_ANTI_EMPRESA = [
            "EMPRESA", "ESPECIALIZADA", "LTDA", "MEI", "EIRELI", "PRODUTORA", 
            "SAAE", "COMERCIO", "SERVICOS", "CONSTRUTORA"
        ]

    async def extract(self):
        log.info("🚀 [v9.3.2] Iniciando D.O. Extractor (Veto Absoluto + Barra de Progresso)")
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []
            
            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: return []
                
            pdf_url = pdf_input.attributes.get("value")
            return await self._processar_pdf(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro crítico no pipeline do D.O.: {e}")
            return []

    async def _processar_pdf(self, pdf_url: str):
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            try:
                resp = await client.get(pdf_url)
                doc = fitz.open(stream=io.BytesIO(resp.content), filetype="pdf")
                texto_completo = ""
                
                # ✅ A BARRA DE PROGRESSO VOLTOU!
                for i in tqdm(range(len(doc)), desc="Lendo PDF do Diário Oficial", unit="pág"):
                    texto_completo += doc.load_page(i).get_text("text") + "\n"
                    if i % 50 == 0: gc.collect()
                    
                doc.close()
                return self._analisar_blocos(texto_completo, pdf_url)
            except Exception as e:
                log.error(f"❌ Falha ao processar o PDF: {e}")
                return []

    def _analisar_blocos(self, texto, url):
        eventos = []
        blocos = self.re_cidade.split(texto)
        
        for i in range(1, len(blocos) - 1, 2):
            cidade = blocos[i].strip().upper()
            conteudo = blocos[i+1].lower() 
            
            # 1. VETO ABSOLUTO
            if any(veto in conteudo for veto in self.PALAVRAS_VETO):
                continue # Pula imediatamente
                
            # 2. SCORING
            score = sum(peso for palavra, peso in self.KEYWORDS_MUSICAIS.items() if palavra in conteudo)
            
            if score >= self.SCORE_THRESHOLD:
                evento = self._extrair_entidades(cidade, blocos[i+1], url, score)
                if evento: eventos.append(evento)
                
        return eventos

    def _extrair_entidades(self, cidade, texto_bloco, url, score):
        texto_limpo = re.sub(r'\s+', ' ', texto_bloco)
        
        # Artista
        match_art = self.re_artista.search(texto_limpo)
        if not match_art: return None
        
        artista = match_art.group(1).strip().upper()
        
        # Filtro Rigoroso de Sujeira
        if len(artista) < 3 or any(t in artista for t in self.FILTRO_ANTI_EMPRESA):
            return None

        # Valor
        match_val = self.re_valor.search(texto_limpo)
        preco = 0.0
        if match_val:
            try: preco = float(match_val.group(1).replace(".", "").replace(",", "."))
            except: pass

        # Data do Evento
        match_data = self.re_data.search(texto_limpo)
        data_ev = datetime.now() + timedelta(days=30)
        if match_data:
            try: data_ev = datetime.strptime(match_data.group(1), "%d/%m/%Y")
            except: pass

        return EventoSchema(
            titulo=f"SHOW: {artista}",
            data_evento=data_ev,
            cidade=cidade,
            local="Evento Público Municipal",
            preco_base=preco,
            fonte=f"AMM-MG",
            url_origem=url,
            vibe="show"
        )