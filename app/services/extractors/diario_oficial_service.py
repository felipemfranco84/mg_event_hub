"""
Padrão de Qualidade: Professional NLP Extraction Pipeline (v8.0.0).
Motivo: Implementação de Blacklist de termos técnicos/obras para evitar falsos positivos
e motor de performance PyMuPDF com monitorização via TQDM.
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

# Lista de termos que indicam contratos que NÃO são de entretenimento
TERMOS_PROIBIDOS = [
    "ASFÁLTICO", "RECAPEAMENTO", "MERENDA", "MEDICAMENTOS", "OBRAS", 
    "REFORMA", "LIMPEZA", "VEÍCULOS", "PEÇAS", "PNEUS", "SOFTWARE", 
    "CONSULTORIA", "SISTEMA", "MANUTENÇÃO", "CONSTRUÇÃO", "TUBULAÇÃO",
    "ELÉTRICA", "HIDRÁULICA", "MÓVEIS", "EQUIPAMENTOS", "PINTURA"
]

class DiarioOficialExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.diariomunicipal.com.br/amm-mg/"
        
        self.meses_pt = {
            "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
            "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
            "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
        }

        # Regex Hub Profissional
        self.re_gatilhos = re.compile(r"(contratação|show|apresentação|inexigibilidade|festa|evento artístico)", re.IGNORECASE)
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        
        # Padrão para isolar o artista (ignora termos genéricos como 'empresa especializada')
        self.re_artista = re.compile(r"(?:DUPLA|BANDA|SHOW(?: ARTÍSTICO)?(?: DE)?|CONTRATAÇÃO (?:DA|DO|DE)|ARTISTA|CANTOR(?:A)?)\s+([A-ZÀ-Ú0-9\s&\'\-]{3,60}?)(?:\s+PARA|\s+NO DIA|,|\s+DURANTE|\.|\s+OBJETO)", re.IGNORECASE)
        
        self.re_data_numerica = re.compile(r"(\d{2}/\d{2}/\d{4})")
        self.re_data_textual = re.compile(r"DIA\s+(\d{1,2})\s+DE\s+([A-ZÀ-Ú]+)\s+DE\s+(\d{4})", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")
        self.re_evento = re.compile(r"(?:FESTA DE|CARNAVAL|ANIVERSÁRIO|EXPO|FESTIVAL|VAQUEJADA)\s+([A-ZÀ-Ú0-9\s]{3,40}?)(?:\.|,|NO MUNICÍPIO|DURANTE)", re.IGNORECASE)

    async def extract(self):
        log.info("🚀 [v8.0] Iniciando Pipeline de Alta Performance no D.O.")
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []

            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: 
                log.error("❌ Falha ao localizar URL do PDF no Diário Oficial.")
                return []

            pdf_url = pdf_input.attributes.get("value")
            return await self._processar_pdf_