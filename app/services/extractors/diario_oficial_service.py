"""
Padrão de Qualidade: Extrator de Elite v11.7.0 (Recall Máximo).
Motivo: Corrigir a falha de associação de valores e duplicação da v11.5.
Ajuste: Fatiamento semântico por bloco de valor (R$) para garantir precisão total.
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
# PARÂMETROS DE INTELIGÊNCIA
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS_ANCORA = r"show\s+musical|show\s+artístico|apresentação\s+artística|contratação\s+de\s+artista|inexigibilidade|festa|aniversário"
PALAVRAS_VETO = ["asfáltic", "pavimentação", "saneamento", "esgoto", "merenda", "peças", "pneus"]

# Regex refinadas para não cortar nomes (v11.7)
RE_ARTISTA = re.compile(
    r"(?:banda|dupla|cantor[a]?|artista|grupo|show(?:\s+com)?)\s+([A-ZÀ-Ú0-9][A-ZÀ-Ú0-9\s&\'\-\.]{3,50})", 
    re.IGNORECASE
)
RE_VALOR = re.compile(r"R\$\s*[\(]?\s*([\d\.]+,\d{2})")
RE_DATA  = re.compile(r"(\d{2}/\d{2}/\d{4})")
RE_CIDADE = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]{3,40})", re.IGNORECASE)

class DiarioOficialExtractor(BaseExtractor):
    BASE_URL = "https://www.diariomunicipal.com.br/amm-mg/"

    async def extract(self) -> list[EventoSchema]:
        log.info("🚀 [v11.7.0] D.O. Extractor — Iniciando Mineração de Alta Precisão")
        try:
            html = await self.fetch_html(self.BASE_URL)
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
        gc.collect()
        return self._extrair_eventos_fatiados(pdf_bytes, pdf_url)

    def _extrair_eventos_fatiados(self, pdf_bytes: bytes, pdf_url: str) -> list[EventoSchema]:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_paginas = len(reader.pages)
            log.info(f"📄 Minerando {total_paginas} páginas...")

            eventos: list[EventoSchema] = []
            hashes_vistos = {} # Usado para manter o maior valor por artista/cidade
            pbar = tqdm(total=total_paginas, desc="Extraindo v11.7", unit="pág")

            cidade_atual = "Minas Gerais"

            for i in range(total_paginas):
                pbar.update(1)
                texto = reader.pages[i].extract_text() or ""
                
                # 1. Atualiza Cidade
                m_cid = RE_CIDADE.search(texto)
                if m_cid: cidade_atual = m_cid.group(1).strip().title()

                # 2. Fatiamento por Bloco de Publicação (Publicado por)
                blocos = re.split(r"Publicado por:", texto, flags=re.IGNORECASE)
                
                for bloco in blocos:
                    if not re.search(KEYWORDS_ANCORA, bloco.lower()): continue
                    if any(v in bloco.lower() for v in PALAVRAS_VETO): continue

                    # 3. Fatiamento Interno por Valor (Resolve múltiplos shows)
                    fatias = re.split(r"(?=R\$\s*[\(]?\s*[\d\.]+,\d{2})", bloco)
                    contexto_acumulado = ""
                    
                    for fatia in fatias:
                        texto_analise = (contexto_acumulado[-300:] + fatia)
                        contexto_acumulado = fatia
                        
                        # Extração de Artista
                        m_art = RE_ARTISTA.search(texto_analise)
                        if not m_art: continue
                        
                        nome = m_art.group(1).strip()
                        # Limpa lixo residual do nome
                        nome = re.split(r'\s+(?:CNPJ|CPF|LTDA|MEI|VALOR|OBJETO|\d{2}\.)', nome, flags=re.IGNORECASE)[0]
                        nome = re.sub(r'^(?:Artística|Musical|Show|Banda|Dupla)\s+', '', nome, flags=re.IGNORECASE).strip().title()
                        
                        if len(nome) < 3: continue

                        # Extração de Valor
                        valor = 0.0
                        m_val = RE_VALOR.search(texto_analise)
                        if m_val:
                            try: valor = float(m_val.group(1).replace(".", "").replace(",", "."))
                            except: pass
                        
                        if valor > 850000: continue

                        # Extração de Data
                        m_dt = RE_DATA.search(texto_analise)
                        data_ev = datetime.now() + timedelta(days=30)
                        if m_dt:
                            try: data_ev = datetime.strptime(m_dt.group(1), "%d/%m/%Y")
                            except: pass

                        tipo = "Show Musical"
                        if "carnaval" in texto_analise.lower(): tipo = "Show Carnavalesco"
                        elif "aniversário" in texto_analise.lower(): tipo = "Aniversário de Cidade"

                        # 4. Deduplicação por maior valor
                        h = f"{nome}-{cidade_atual}"
                        if h not in hashes_vistos or valor > hashes_vistos[h].preco_base:
                            hashes_vistos[h] = EventoSchema(
                                titulo=f"{tipo}: {nome}",
                                data_evento=data_ev,
                                cidade=cidade_atual,
                                local=f"Município de {cidade_atual}",
                                preco_base=valor,
                                fonte="AMM-MG (v11.7.0)",
                                url_origem=pdf_url,
                                vibe="show"
                            )

                if i % 30 == 0: gc.collect()

            pbar.close()
            eventos = list(hashes_vistos.values())
            log.info(f"✅ Sucesso! {len(eventos)} eventos únicos minerados.")
            return eventos
        except Exception as e:
            if 'pbar' in locals(): pbar.close()
            log.error(f"❌ Falha: {e}")
            return []
