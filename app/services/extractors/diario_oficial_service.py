"""
Padrão de Qualidade: High-Performance PDF Mining (v7.0.0).
Motivo: Migração para PyMuPDF (fitz) para viabilizar processamento em e2-micro.
Inclui barra de progresso visual e gerenciamento agressivo de memória.
"""
import re
import io
import httpx
import fitz  # PyMuPDF
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
        
        self.meses_pt = {
            "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
            "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
            "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
        }

        # Regex Hub Otimizado
        self.re_gatilhos = re.compile(r"(contratação|show|apresentação|inexigibilidade|festa)", re.IGNORECASE)
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        self.re_artista = re.compile(r"(?:DUPLA|BANDA|SHOW(?: ARTÍSTICO)?(?: DE)?|CONTRATAÇÃO (?:DA|DO|DE)|ARTISTA)\s+([A-ZÀ-Ú0-9\s&\'\-]+?)(?:\s+PARA|\s+NO DIA|,|\s+DURANTE|\.)", re.IGNORECASE)
        self.re_data_numerica = re.compile(r"(\d{2}/\d{2}/\d{4})")
        self.re_data_textual = re.compile(r"DIA\s+(\d{1,2})\s+DE\s+([A-ZÀ-Ú]+)\s+DE\s+(\d{4})", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")
        self.re_evento = re.compile(r"(?:FESTA DE|CARNAVAL|ANIVERSÁRIO|EXPO|FESTIVAL)\s+([A-ZÀ-Ú0-9\s]+?)(?:\.|,|NO MUNICÍPIO)", re.IGNORECASE)

    async def extract(self):
        log.info("🚀 Iniciando Pipeline High-Performance no D.O. (AMM-MG)")
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []

            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: return []

            pdf_url = pdf_input.attributes.get("value")
            return await self._processar_pdf_performante(pdf_url)

        except Exception as e:
            log.error(f"❌ Falha crítica: {e}")
            return []

    async def _processar_pdf_performante(self, pdf_url: str):
        eventos_validados = []
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            resp = await client.get(pdf_url)
            resp.raise_for_status()
            
            # Abre o documento via streaming de memória
            doc = fitz.open(stream=io.BytesIO(resp.content), filetype="pdf")
            texto_completo = ""
            
            log.info(f"📊 Processando {len(doc)} páginas via PyMuPDF...")
            
            # Barra de progresso visível no terminal
            for i in tqdm(range(len(doc)), desc="Lendo Páginas", unit="pag"):
                page = doc.load_page(i)
                texto_completo += page.get_text("text") + "\n"
                
                # Limpeza periódica de memória (Garbage Collection)
                if i % 50 == 0:
                    gc.collect()

            doc.close()
            eventos_validados = self._analisar_texto(texto_completo, pdf_url)
            gc.collect()
            return eventos_validados

    def _analisar_texto(self, texto, url):
        encontrados = []
        blocos = self.re_cidade.split(texto)
        
        for i in range(1, len(blocos) - 1, 2):
            cidade = blocos[i].strip().upper()
            conteudo = blocos[i+1]
            
            if self.re_gatilhos.search(conteudo):
                texto_limpo = re.sub(r'\s+', ' ', conteudo)
                match_art = self.re_artista.search(texto_limpo)
                
                if match_art:
                    artista = match_art.group(1).strip()
                    if len(artista) > 60 or any(x in artista for x in ["EXTRATO", "PROCESSO"]): continue
                    
                    match_val = self.re_valor.search(texto_limpo)
                    preco = float(match_val.group(1).replace(".", "").replace(",", ".")) if match_val else 0.0
                    
                    match_ev = self.re_evento.search(texto_limpo)
                    nome_ev = match_ev.group(1).strip() if match_ev else "Evento Municipal"
                    
                    encontrados.append(EventoSchema(
                        titulo=f"SHOW: {artista} ({nome_ev})",
                        data_evento=self._extrair_data(texto_limpo),
                        cidade=cidade,
                        local="Praça Pública",
                        preco_base=preco,
                        fonte="amm_mg_pdf",
                        url_origem=url,
                        vibe="show"
                    ))
        return encontrados

    def _extrair_data(self, texto):
        m_txt = self.re_data_textual.search(texto)
        if m_txt:
            try:
                dt = datetime(int(m_txt.group(3)), self.meses_pt.get(m_txt.group(2).lower(), 1), int(m_txt.group(1)))
                if dt > datetime.now(): return dt
            except: pass
        
        m_num = self.re_data_numerica.search(texto)
        if m_num:
            try:
                dt = datetime.strptime(m_num.group(1), "%d/%m/%Y")
                if dt > datetime.now(): return dt
            except: pass
        return datetime.now() + timedelta(days=45)