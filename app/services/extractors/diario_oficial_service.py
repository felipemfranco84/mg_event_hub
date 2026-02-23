"""
Padrão de Qualidade: Streaming Pipeline com Fatiamento Semântico e UI de Terminal (v11.1.1).
Motivo: Unificação final entre economia de hardware (GCP 1GB RAM) e precisão de extração.
Correções v11.1.1:
- Reintrodução da barra de progresso (tqdm) no loop de janelas.
- Correção de SyntaxWarning nas strings de Regex (Raw Strings).
- Manutenção do Veto Absoluto para expurgar infraestrutura e fatiamento por R$.
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

MAX_PDF_BYTES       = 50 * 1024 * 1024  # 50 MB
JANELA_PAGINAS      = 3                 
GC_INTERVALO        = 30                
SCORE_THRESHOLD     = 45

# Veto Absoluto: Impede falsos positivos de infraestrutura instantaneamente
PALAVRAS_VETO = [
    "asfáltic", "recapeamento", "pavimentação", "saneamento", "esgoto", 
    "peças e acessórios", "pneus", "lubrificantes", "merenda", "medicamentos", 
    "informática", "software", "manutenção preventiva", "manutenção corretiva",
    "brita", "engenharia", "terraplanagem", "drenagem", "sinalização viária"
]

KEYWORDS_TRIGGER = {
    "show artístico": 60, "show musical": 60, "apresentação artística": 55, 
    "contratação de artista": 55, "banda musical": 50, "dupla sertaneja": 50, 
    "grupo musical": 50, "apresentação musical": 50,
}

KEYWORDS_REFORCO = {
    "carnaval": 30, "festa junina": 30, "festival literário": 25,
    "festa do município": 25, "aniversário do município": 25,
    "festividade": 20, "cachê": 20, "palco": 15, "inexigibilidade": 10, 
    "evento cultural": 20, "circo": 20, "teatro": 15, "forró": 20, "samba": 15,
}

# ─────────────────────────────────────────────────────────────────────────────
# Expressões Regulares (Compiladas como Raw Strings para evitar SyntaxWarnings)
# ─────────────────────────────────────────────────────────────────────────────

RE_PREFEITURA = re.compile(
    r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú][A-ZÀ-Ú\s\-\/]{2,40}?)"
    r"(?:\n|/MG|,|\s{2,}|SECRETARIA|DEPARTAMENTO|CÂMARA)",
    re.IGNORECASE
)

RE_ARTISTA = re.compile(
    r"(?:SHOW\s+ART[IÍ]STICO\s+COM|SHOW\s+MUSICAL\s+COM|APRESENTA[ÇC][ÃA]O\s+(?:MUSICAL|ART[IÍ]STICA)\s+COM|CONTRATAD[OA]:?|OBJETO:?.*?CONTRATAÇÃO DE|BANDA|DUPLA|CANTOR[A]?)\s*(?:A\s+EMPRESA|O\s+ARTISTA)?\s*([A-ZÀ-Ú0-9][A-ZÀ-Ú0-9\s&\'\-\.]{3,50}?)(?:[\"»""]|\s+PARA|\s+NO\s+DIA|\s+A\s+SER|\s+CNPJ|,|\.|;|/|-)",
    re.IGNORECASE
)

RE_DATA = re.compile(
    r"(?:REALIZADO\s+(?:NO\s+DIA\s+)?|NO\s+DIA\s+|DATA\s+DO\s+EVENTO[:\s]+)"
    r"(\d{1,2}[\s\/\-\.]\d{1,2}[\s\/\-\.]\d{2,4})",
    re.IGNORECASE
)

RE_VALOR = re.compile(r"R\$\s*[\(]?\s*([\d\.]+,\d{2})")

_LIXO_ARTISTA = [
    "EMPRESA", "LTDA", "S/A", "ESPECIALIZADA", "CONTRATAÇÃO",
    "SERVIÇO", "PESSOA FÍSICA", "PESSOA JURÍDICA", "SAAE", "PRODUTORA", "PROMOÇÕES"
]

# ─────────────────────────────────────────────────────────────────────────────
# Funções Utilitárias (Stateless)
# ─────────────────────────────────────────────────────────────────────────────

def _calcular_score(texto: str) -> tuple[int, list[str]]:
    score, encontradas = 0, []
    for kw, peso in KEYWORDS_TRIGGER.items():
        if kw in texto:
            score += peso
            encontradas.append(f"TRIGGER:{kw}")
    for kw, peso in KEYWORDS_REFORCO.items():
        if kw in texto:
            score += peso
            encontradas.append(f"REFORÇO:{kw}")
    return score, encontradas

def _extrair_artista(texto: str) -> Optional[str]:
    m = RE_ARTISTA.search(texto)
    if m:
        nome = re.sub(r'\s+', ' ', m.group(1)).strip('"\'«»""')
        if len(nome) >= 3 and not any(l in nome.upper() for l in _LIXO_ARTISTA):
            return nome.upper()
    return None

def _extrair_valor(texto: str) -> float:
    m = RE_VALOR.search(texto)
    if m:
        try:
            return float(m.group(1).replace(".", "").replace(",", "."))
        except:
            pass
    return 0.0

def _extrair_data(texto: str) -> Optional[str]:
    m = RE_DATA.search(texto)
    return m.group(1).strip() if m else None

def _classificar_vibe(texto: str) -> str:
    t = texto.lower()
    if any(w in t for w in ["sertanejo", "forró", "xote", "country", "dupla"]): return "sertanejo"
    if any(w in t for w in ["festival", "cultural", "teatro", "exposição"]): return "festival"
    return "show"

def _gerar_janelas(reader, janela: int = JANELA_PAGINAS):
    buffer = []
    total = len(reader.pages)
    for i in range(total):
        texto_pagina = reader.pages[i].extract_text() or ""
        buffer.append(texto_pagina)
        if len(buffer) > janela:
            buffer.pop(0)
        if len(buffer) == janela or i == total - 1:
            yield "\n".join(buffer), i
        if i % GC_INTERVALO == 0 and i > 0:
            gc.collect()
    buffer.clear()
    gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# Classe Principal
# ─────────────────────────────────────────────────────────────────────────────

class DiarioOficialExtractor(BaseExtractor):
    BASE_URL = "https://www.diariomunicipal.com.br/amm-mg/"

    async def extract(self) -> list[EventoSchema]:
        log.info("🚀 [v11.1.1] D.O. Extractor — Iniciando Pipeline de Memória Inteligente")
        try:
            html = await self.fetch_html(self.BASE_URL)
            if not html: return []

            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: return []

            pdf_url = pdf_input.attributes.get("value", "")
            return await self._processar_pdf_streaming(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro crítico no D.O. Extractor: {e}")
            return []

    async def _processar_pdf_streaming(self, pdf_url: str) -> list[EventoSchema]:
        log.info(f"📥 Baixando PDF do dia...")
        chunks: list[bytes] = []
        total_bytes = 0

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=120.0, headers=self.get_headers()) as client:
                async with client.stream("GET", pdf_url) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.aiter_bytes(chunk_size=65_536):
                        chunks.append(chunk)
                        total_bytes += len(chunk)
                        if total_bytes > MAX_PDF_BYTES:
                            log.error("❌ PDF excede limite de segurança (50MB) — abortando")
                            return []

            log.info(f"✅ Download concluído: {total_bytes / 1024:.0f} KB")
            pdf_bytes = b"".join(chunks)
            del chunks
            gc.collect()

            return self._extrair_eventos_streaming(pdf_bytes, pdf_url)
        except Exception as e:
            log.error(f"❌ Falha ao processar streaming do PDF: {e}")
            return []

    def _extrair_eventos_streaming(self, pdf_bytes: bytes, pdf_url: str) -> list[EventoSchema]:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_paginas = len(reader.pages)
            log.info(f"📄 Minerando {total_paginas} páginas...")

            del pdf_bytes
            gc.collect()

            eventos: list[EventoSchema] = []
            hashes_vistos: set[str] = set()
            
            # Inicializa a barra de progresso
            pbar = tqdm(total=total_paginas, desc="Processando D.O.", unit="pág", leave=True)

            for janela_texto, pag_idx in _gerar_janelas(reader, JANELA_PAGINAS):
                # Atualiza a UI do terminal
                pbar.n = pag_idx + 1
                pbar.refresh()

                # Limpeza de ruído de cabeçalho AMM-MG para evitar quebras de Regex
                janela_limpa = re.sub(r"Minas Gerais\s*,\s*\d{2}.*?www\.diariomunicipal\.com\.br/amm-mg\s*\d+", "", janela_texto)
                blocos_mun = RE_PREFEITURA.split(janela_limpa)

                for i in range(1, len(blocos_mun) - 1, 2):
                    cidade = blocos_mun[i].strip().upper()
                    conteudo = blocos_mun[i+1]

                    # ✅ FATIAMENTO MONETÁRIO: Divide o bloco de texto cada vez que encontrar um cifrão
                    fatias = re.split(r"(?=R\$\s*[\(]?\s*[\d\.]+,\d{2})", conteudo)
                    contexto_acumulado = ""

                    for fatia in fatias:
                        # Mantém janela de contexto para não perder a âncora do artista (ex: "Contratação da Banda X...")
                        texto_analise = (contexto_acumulado[-250:] + fatia).lower()
                        contexto_acumulado = fatia

                        # ✅ VETO ABSOLUTO: Descarta imediatamente se for infraestrutura ou merenda
                        if any(veto in texto_analise for veto in PALAVRAS_VETO):
                            continue

                        score, keywords = _calcular_score(texto_analise)
                        if score < SCORE_THRESHOLD:
                            continue

                        artista = _extrair_artista(texto_analise)
                        if not artista:
                            continue

                        valor = _extrair_valor(texto_analise)
                        data_str = _extrair_data(texto_analise)
                        data_evento = datetime.now() + timedelta(days=30)
                        
                        if data_str:
                            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
                                try:
                                    data_evento = datetime.strptime(data_str[:10].replace("-", "/"), "%d/%m/%Y")
                                    break
                                except ValueError:
                                    pass

                        # Deduplicação via Hash
                        h = hashlib.md5(f"{artista}{cidade}".encode()).hexdigest()[:12]
                        if h not in hashes_vistos:
                            hashes_vistos.add(h)
                            eventos.append(EventoSchema(
                                titulo=f"SHOW: {artista}",
                                data_evento=data_evento,
                                cidade=cidade,
                                local="Evento Municipal / Praça Pública",
                                preco_base=valor,
                                fonte=f"AMM-MG (v11.1)",
                                url_origem=pdf_url,
                                vibe=_classificar_vibe(texto_analise)
                            ))
                            # Log opcional para acompanhar em tempo real abaixo da barra
                            # log.debug(f"✨ Encontrado: {artista} em {cidade}")

            pbar.close()
            log.info(f"✅ Processamento finalizado. {len(eventos)} eventos extraídos com sucesso.")
            return eventos

        except Exception as e:
            if 'pbar' in locals(): pbar.close()
            log.error(f"❌ Falha na extração por streaming: {e}")
            return []
        finally:
            gc.collect()