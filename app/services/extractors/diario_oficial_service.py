"""
Padrão de Qualidade: Professional NLP Extraction Pipeline (v8.8.0).
Motivo: Blacklist reforçada para eliminar ruído de licitações comuns (asfalto, merenda, veículos).
"""
import re
import io
import httpx
import fitz
import gc
from tqdm import tqdm
from datetime import datetime, timedelta
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

# ✅ Lista negra expandida para filtrar o lixo do D.O.
TERMOS_PROIBIDOS = [
    "ASFÁLTICO", "RECAPEAMENTO", "MERENDA", "MEDICAMENTOS", "OBRAS",
    "REFORMA", "LIMPEZA", "VEÍCULOS", "PEÇAS", "PNEUS", "SOFTWARE",
    "CONSULTORIA", "SISTEMA", "MANUTENÇÃO", "CONSTRUÇÃO", "TUBULAÇÃO",
    "ELÉTRICA", "HIDRÁULICA", "MÓVEIS", "EQUIPAMENTOS", "PINTURA", 
    "CASCALHAMENTO", "EMPRESA", "PESSOA JURÍDICA", "PESSOA FÍSICA",
    "FORNECEDORES", "PRESTAÇÃO DE SERVIÇOS", "AQUISIÇÃO", "BENS", "MICROCHIPS",
    "REVISÃO", "ACESSÓRIOS", "PEÇAS", "DIESEL", "GASOLINA"
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
        self.re_gatilhos = re.compile(r"(contratação|show|apresentação|inexigibilidade|festa|evento artístico)", re.IGNORECASE)
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        self.re_artista = re.compile(r"(?:DUPLA|BANDA|SHOW(?: ARTÍSTICO)?(?: DE)?|CONTRATAÇÃO (?:DA|DO|DE)|ARTISTA|CANTOR(?:A)?)\s+([A-ZÀ-Ú0-9\s&\'\-]{3,60}?)(?:\s+PARA|\s+NO DIA|,|\s+DURANTE|\.|\s+OBJETO)", re.IGNORECASE)
        self.re_data_numerica = re.compile(r"(\d{2}/\d{2}/\d{4})")
        self.re_data_textual = re.compile(r"DIA\s+(\d{1,2})\s+DE\s+([A-ZÀ-Ú]+)\s+DE\s+(\d{4})", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")
        self.re_evento = re.compile(r"(?:FESTA DE|CARNAVAL|ANIVERSÁRIO|EXPO|FESTIVAL|VAQUEJADA)\s+([A-ZÀ-Ú0-9\s]{3,40}?)(?:\.|,|NO MUNICÍPIO|DURANTE)", re.IGNORECASE)

    async def extract(self):
        log.info("🚀 [v8.8] Minerando D.O. com Blacklist Reforçada")
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []
            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: return []
            pdf_url = pdf_input.attributes.get("value")
            return await self._processar_pdf_performante(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro na extração: {e}")
            return []

    async def _processar_pdf_performante(self, pdf_url: str):
        async with httpx.AsyncClient(follow_redirects=True, timeout=180.0) as client:
            try:
                resp = await client.get(pdf_url)
                doc = fitz.open(stream=io.BytesIO(resp.content), filetype="pdf")
                texto_completo = ""
                for i in tqdm(range(len(doc)), desc="Lendo PDF", unit="pag"):
                    texto_completo += doc.load_page(i).get_text("text") + "\n"
                    if i % 50 == 0: gc.collect()
                doc.close()
                return self._analisar_texto(texto_completo, pdf_url)
            except Exception as e:
                log.error(f"❌ Erro PDF: {e}")
                return []

    def _analisar_texto(self, texto, url):
        encontrados = []
        blocos = self.re_cidade.split(texto)
        for i in range(1, len(blocos) - 1, 2):
            cidade = blocos[i].strip().upper()
            conteudo = blocos[i+1]
            if self.re_gatilhos.search(conteudo):
                evento = self._extrair_entidades(cidade, conteudo, url)
                if evento: encontrados.append(evento)
        return encontrados

    def _extrair_entidades(self, cidade, texto_bloco, url):
        texto_limpo = re.sub(r'\s+', ' ', texto_bloco)
        match_art = self.re_artista.search(texto_limpo)
        if not match_art: return None

        artista = match_art.group(1).strip().upper()

        # ✅ FILTRO DE ELITE: Se o "artista" contém termos de empresa ou asfalto, descarta.
        if any(proibido in artista for proibido in TERMOS_PROIBIDOS):
            return None
        
        # Filtro adicional para evitar nomes genéricos demais
        if len(artista) < 3 or artista.isdigit():
            return None

        match_val = self.re_valor.search(texto_limpo)
        preco = 0.0
        if match_val:
            try:
                preco = float(match_val.group(1).replace(".", "").replace(",", "."))
            except: pass

        match_ev = self.re_evento.search(texto_limpo)
        nome_ev = match_ev.group(1).strip() if match_ev else "Evento Municipal"

        return EventoSchema(
            titulo=f"SHOW: {artista} ({nome_ev})",
            data_evento=self._extrair_data(texto_limpo),
            cidade=cidade,
            local="Praça Pública",
            preco_base=preco,
            fonte="amm_mg_pdf",
            url_origem=url,
            vibe="show"
        )

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