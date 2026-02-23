"""
Padrão de Qualidade: Streaming NLP com Parser de Listas e Limpeza de Entidades (v11.2.0).
Motivo: Superar o recall da v11.1 capturando listas corridas de artistas (Caso Cássia)
e tratando nomes com CPFs/CNPJs acoplados, mantendo o baixo consumo de RAM.
"""

import re
import gc
import io
import httpx
import hashlib
from tqdm import tqdm
from datetime import datetime, timedelta
from typing import Optional

from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

try:
    from pypdf import PdfReader
except ImportError:
    raise ImportError("pypdf não encontrado. Execute: pip install pypdf")

try:
    from selectolax.parser import HTMLParser
except ImportError:
    raise ImportError("selectolax não encontrado. Execute: pip install selectolax")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes de Infraestrutura e NLP
# ─────────────────────────────────────────────────────────────────────────────

MAX_PDF_BYTES       = 50 * 1024 * 1024
JANELA_PAGINAS      = 3                 
GC_INTERVALO        = 30                
SCORE_THRESHOLD     = 40

PALAVRAS_VETO = [
    "asfáltic", "recapeamento", "pavimentação", "saneamento", "esgoto", 
    "peças e acessórios", "pneus", "lubrificantes", "merenda", "medicamentos", 
    "informática", "software", "manutenção", "brita", "engenharia", "limpeza urbana"
]

KEYWORDS_TRIGGER = {
    "show artístico": 60, "show musical": 60, "apresentação artística": 55, 
    "contratação de artista": 55, "banda musical": 50, "dupla sertaneja": 50, 
    "grupo musical": 50, "apresentação musical": 50, "inexigibilidade": 10,
}

# ─────────────────────────────────────────────────────────────────────────────
# Regex de Elite (Tratamento de CPF e Listas)
# ─────────────────────────────────────────────────────────────────────────────

RE_PREFEITURA = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú][A-ZÀ-Ú\s\-\/]{2,40}?)", re.IGNORECASE)

# Regex que limpa CPF/CNPJ grudado no nome
RE_LIMPAR_DOC = re.compile(r"\d{3,}\.?\d{3,}.*")

RE_ARTISTA = re.compile(
    r"(?:SHOW\s+ART[IÍ]STICO\s+COM|SHOW\s+MUSICAL\s+COM|APRESENTA[ÇC][ÃA]O\s+(?:MUSICAL|ART[IÍ]STICA)\s+COM|CONTRATAD[OA]:?|OBJETO:?.*?CONTRATAÇÃO DE|BANDA|DUPLA|CANTOR[A]?)\s*(?:A\s+EMPRESA|O\s+ARTISTA)?\s*([A-ZÀ-Ú0-9][A-ZÀ-Ú0-9\s&\'\-\.]{3,50}?)",
    re.IGNORECASE
)

RE_DATA = re.compile(r"(\d{2}/\d{2}/\d{4})")
RE_VALOR = re.compile(r"R\$\s*[\(]?\s*([\d\.]+,\d{2})")

_LIXO_ARTISTA = ["EMPRESA", "LTDA", "S/A", "ESPECIALIZADA", "SAAE", "PRODUTORA", "PROMOÇÕES", "CNPJ", "CPF"]

# ─────────────────────────────────────────────────────────────────────────────
# Funções de Inteligência
# ─────────────────────────────────────────────────────────────────────────────

def _calcular_score(texto: str) -> int:
    score = 0
    t = texto.lower()
    for kw, peso in KEYWORDS_TRIGGER.items():
        if kw in t: score += peso
    return score

def _limpar_nome_artista(nome: str) -> str:
    # Remove CPF/CNPJ que venha grudado no nome
    nome = RE_LIMPAR_DOC.sub('', nome)
    # Remove lixo comum
    for termo in _LIXO_ARTISTA:
        nome = nome.replace(termo, "")
    return re.sub(r'\s+', ' ', nome).strip().upper()

def _gerar_janelas(reader, janela: int = JANELA_PAGINAS):
    buffer = []
    for i in range(len(reader.pages)):
        texto_pagina = reader.pages[i].extract_text() or ""
        buffer.append(texto_pagina)
        if len(buffer) > janela: buffer.pop(0)
        if len(buffer) == janela or i == len(reader.pages) - 1:
            yield "\n".join(buffer), i
        if i % GC_INTERVALO == 0: gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# Classe Principal
# ─────────────────────────────────────────────────────────────────────────────

class DiarioOficialExtractor(BaseExtractor):
    async def extract(self) -> list[EventoSchema]:
        log.info("🚀 [v11.2.0] D.O. Extractor — X1 de Extração Ativado")
        try:
            html = await self.fetch_html("https://www.diariomunicipal.com.br/amm-mg/")
            tree = HTMLParser(html)
            pdf_url = tree.css_first("input#urlPdf").attributes.get("value", "")
            return await self._processar_pdf_streaming(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro: {e}")
            return []

    async def _processar_pdf_streaming(self, pdf_url: str) -> list[EventoSchema]:
        chunks = []
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            async with client.stream("GET", pdf_url) as resp:
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
        
        pdf_bytes = b"".join(chunks)
        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_paginas = len(reader.pages)
        
        eventos = []
        hashes_vistos = set()
        pbar = tqdm(total=total_paginas, desc="X1 Minerando", unit="pág")

        for janela_texto, pag_idx in _gerar_janelas(reader):
            pbar.n = pag_idx + 1
            pbar.refresh()

            # Limpeza de rodapé e normalização
            texto_limpo = re.sub(r"Minas Gerais.*?\d+", "", janela_texto)
            blocos_mun = RE_PREFEITURA.split(texto_limpo)

            for i in range(1, len(blocos_mun) - 1, 2):
                cidade = blocos_mun[i].strip().upper()
                conteudo = blocos_mun[i+1]

                # VETO IMEDIATO
                if any(veto in conteudo.lower() for veto in PALAVRAS_VETO):
                    continue

                # FATIAMENTO POR CONTRATO (Melhorado com janela de 400 chars)
                fatias = re.split(r"(?=R\$\s*[\(]?\s*[\d\.]+,\d{2})", conteudo)
                contexto_acumulado = ""

                for fatia in fatias:
                    texto_analise = (contexto_acumulado[-400:] + fatia)
                    contexto_acumulado = fatia
                    
                    if _calcular_score(texto_analise) < SCORE_THRESHOLD:
                        continue

                    # Extração de Artista
                    match_art = RE_ARTISTA.search(texto_analise)
                    if not match_art: continue
                    
                    artista = _limpar_nome_artista(match_art.group(1))
                    if len(artista) < 3: continue

                    # Extração de Valor e Data
                    valor = 0.0
                    m_val = RE_VALOR.search(texto_analise)
                    if m_val:
                        valor = float(m_val.group(1).replace(".", "").replace(",", "."))

                    m_dt = RE_DATA.search(texto_analise)
                    data_ev = datetime.now() + timedelta(days=30)
                    if m_dt:
                        try: data_ev = datetime.strptime(m_dt.group(1), "%d/%m/%Y")
                        except: pass

                    h = hashlib.md5(f"{artista}{cidade}{valor}".encode()).hexdigest()[:12]
                    if h not in hashes_vistos:
                        hashes_vistos.add(h)
                        eventos.append(EventoSchema(
                            titulo=f"SHOW: {artista}",
                            data_evento=data_ev,
                            cidade=cidade,
                            local="Evento Municipal",
                            preco_base=valor,
                            fonte="AMM-MG (v11.2)",
                            url_origem=pdf_url,
                            vibe="show"
                        ))
        pbar.close()
        log.info(f"🏁 X1 Finalizado! {len(eventos)} eventos extraídos.")
        return eventos