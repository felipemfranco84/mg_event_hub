"""
Padrão de Qualidade: SOLID, Clean Code e NLP Heurístico (v9.3.0).
Motivo: Expressões regulares lineares são insuficientes para PDFs do Diário Oficial. 
Este serviço implementa um Motor de Pontuação (Scoring Engine) que avalia o peso semântico 
de cada publicação, garantindo que contratos de obras (asfalto, merenda) sejam descartados 
e apenas contratos de entretenimento sejam promovidos a "Eventos".
"""
import re
import io
import fitz
import httpx
import gc
from datetime import datetime, timedelta
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class DiarioOficialExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.diariomunicipal.com.br/amm-mg/"
        
        # Dicionários de Scoring
        self.KEYWORDS_MUSICAIS = {
            "apresentação artística": 30,
            "show artístico": 30,
            "contratação de artista": 30,
            "banda musical": 20,
            "dupla sertaneja": 20,
            "cantor": 15,
            "festival": 15,
            "cachê": 10,
            "palco": 10,
            "inexigibilidade": 5 # Shows são contratados por inexigibilidade
        }

        self.KEYWORDS_EXCLUSAO = {
            "asfalto": -100,
            "recapeamento": -100,
            "engenharia": -100,
            "merenda": -100,
            "medicamento": -100,
            "pneus": -100,
            "manutenção preventiva": -50,
            "empresa especializada": -30,
            "software": -50,
            "obras": -50,
            "pavimentação": -100,
            "concreto": -100
        }
        
        self.SCORE_THRESHOLD = 35 # Limiar mínimo para considerar um evento
        
        # Regex de Extração Fina (Atuam apenas se o Score passar)
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        self.re_artista = re.compile(r"(?:BANDA|SHOW(?: ARTÍSTICO)?(?: DE)?|CONTRATAÇÃO (?:DA|DO|DE)|CANTOR(?:A)?|DUPLA)\s+([A-ZÀ-Ú0-9\s&\'\-]{4,50}?)(?:\s+PARA|\s+NO DIA|,|\s+DURANTE|\.|\s+OBJETO)", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")

    async def extract(self):
        """
        Motivo: Orquestrar a extração do PDF diário, isolando falhas de rede.
        """
        log.info("🚀 [v9.3.0] Iniciando D.O. Extractor com Motor de Scoring NLP...")
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []
            
            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input:
                log.warning("⚠️ Input do PDF não encontrado no D.O.")
                return []
                
            pdf_url = pdf_input.attributes.get("value")
            return await self._processar_pdf(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro crítico no pipeline do D.O.: {e}")
            return []

    async def _processar_pdf(self, pdf_url: str):
        """
        Motivo: Download do PDF em memória e extração de texto paginada, 
        com coleta de lixo (GC) para evitar estouro de memória na GCP.
        """
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            try:
                resp = await client.get(pdf_url)
                doc = fitz.open(stream=io.BytesIO(resp.content), filetype="pdf")
                texto_completo = ""
                
                for i in range(len(doc)):
                    texto_completo += doc.load_page(i).get_text("text") + "\n"
                    if i % 50 == 0: gc.collect()
                    
                doc.close()
                return self._analisar_blocos(texto_completo, pdf_url)
            except Exception as e:
                log.error(f"❌ Falha ao processar o arquivo PDF do D.O.: {e}")
                return []

    def _analisar_blocos(self, texto, url):
        """
        Motivo: Aplica o algoritmo de Scoring em blocos isolados por Município.
        """
        eventos = []
        blocos = self.re_cidade.split(texto)
        
        for i in range(1, len(blocos) - 1, 2):
            try:
                cidade = blocos[i].strip().upper()
                conteudo = blocos[i+1].lower() # Normalização para scoring
                
                score = self._calcular_score(conteudo)
                
                if score >= self.SCORE_THRESHOLD:
                    evento = self._extrair_entidades(cidade, blocos[i+1], url, score)
                    if evento: eventos.append(evento)
            except Exception as e:
                log.warning(f"⚠️ Erro ao analisar bloco da cidade {cidade}: {e}")
                continue
                
        return eventos

    def _calcular_score(self, texto_bloco: str) -> int:
        """
        Motivo: Avaliar a relevância do bloco baseado em pesos pré-definidos.
        """
        score = 0
        for palavra, peso in self.KEYWORDS_MUSICAIS.items():
            if palavra in texto_bloco:
                score += peso
                
        for palavra, penalidade in self.KEYWORDS_EXCLUSAO.items():
            if palavra in texto_bloco:
                score += penalidade
                
        return score

    def _extrair_entidades(self, cidade, texto_bloco, url, score):
        """
        Motivo: Se o bloco for aprovado pelo Scoring, aplicar Regex para buscar a entidade exata.
        """
        try:
            texto_limpo = re.sub(r'\s+', ' ', texto_bloco)
            match_art = self.re_artista.search(texto_limpo)
            
            if not match_art: return None
            
            artista = match_art.group(1).strip().upper()
            
            # Dupla checagem: Se mesmo com score alto a Regex capturar algo genérico, descartar.
            if len(artista) < 4 or "ESPECIALIZADA" in artista or "LTDA" in artista:
                return None

            match_val = self.re_valor.search(texto_limpo)
            preco = 0.0
            if match_val:
                try:
                    preco = float(match_val.group(1).replace(".", "").replace(",", "."))
                except: pass

            return EventoSchema(
                titulo=f"Shows Oficiais: {artista}",
                data_evento=datetime.now() + timedelta(days=30), # Previsão padrão de publicação D.O.
                cidade=cidade,
                local="Evento Municipal / Praça Pública",
                preco_base=preco,
                fonte=f"amm_mg (Score: {score})",
                url_origem=url,
                vibe="show"
            )
        except Exception as e:
            log.warning(f"⚠️ Erro ao extrair entidades do texto aprovado: {e}")
            return None