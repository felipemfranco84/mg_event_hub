"""
Padrão de Qualidade: Fusão de Infraestrutura v11 com Lógica Cultural v2.2 (v11.4.1).
Motivo: Implementar a lógica de janelas assimétricas e âncoras culturais validada pelo usuário,
mantendo a eficiência de memória para 1GB RAM na GCP.
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
# Lógica Cultural v2.2 - Configurações de Extração
# ─────────────────────────────────────────────────────────────────────────────

MAX_PDF_BYTES   = 50 * 1024 * 1024
JANELA_PAGINAS  = 3
GC_INTERVALO    = 30

# Âncoras de Inclusão e Exclusão (100% fiéis ao script v2.2)
KEYWORDS_ANCORA = [
    r"show\s+musical", r"show\s+artístico", r"apresentação\s+artística", 
    r"apresentação\s+musical", r"serviços\s+artísticos", r"calendário\s+cultural",
    r"credenciamento.*(?:artista|banda|músic)", r"grupo\s+musical",
    r"festival\s+(?:gastronômico|cultural|de\s+\w)", r"festa\s+(?:junina|municipal|de\s+agosto)",
    r"aniversário.*(?:cidade|município)", r"aulas?\s+de\s+(?:zumba|dança|karatê|atletismo|música|canto)",
    r"espetáculo\s+(?:artístico|cultural|teatral)", r"contratação.*artista",
    r"contratação\s+da\s+(?:banda|dupla)", r"festividades.*carnaval",
    r"banda\s+musical\s+composta", r"artistas?\s+musicais?"
]

VETO_ANCORA = [
    r"pavimentação", r"saneamento", r"esgoto", r"merenda\s+escolar", 
    r"combustíveis", r"seguro\s+automotivo", r"análises\s+clínicas",
    r"serviços\s+médicos", r"tecnologia\s+da\s+informação",
    r"sinalização\s+viária", r"obras\s+de\s+engenharia",
    r"Minha Casa Minha Vida", r"habita"
]

BLACKLIST_ARTISTA = {
    "composta", "artística", "musical", "local", "estadual", "nacional",
    "renome local", "renome estadual", "renome nacional",
    "artistasde renome locale regional", "artistasde renome estadual",
    "renome locale regional"
}

# ─────────────────────────────────────────────────────────────────────────────
# Regex de Extração de Nomes (Padrões v2.2)
# ─────────────────────────────────────────────────────────────────────────────

PADROES_ARTISTA = [
    re.compile(r'(?:da\s+banda|do\s+cantor[a]?|da\s+dupla|do\s+grupo)\s+([A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ0-9][A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇa-záéíóúàèìòùãõâêîôûç0-9\s&\'\-\.]{1,60}?)(?=\s*[,\.\;\n\r]|\s+(?:com\s+dur|para\s+|por\s+|durante|através|no\s+|na\s+|inscrit|CNPJ|CPF|deten|por\s+meio))', re.IGNORECASE),
    re.compile(r'artista\s+["\u201c\u201d\u2018\u2019]([^""\u201c\u201d\u2018\u2019\n\r]+)["\u201c\u201d\u2018\u2019]', re.IGNORECASE),
    re.compile(r'(?:com|show\s+artístico\s+musical\s+com)[―\u2019\u201c"]\s*([A-ZÀ-Ú][^―\u201d\u201c"\n\r]{3,80}?)[‖\u201d"]', re.IGNORECASE),
    re.compile(r'contratação\s+da\s+(?:banda|dupla)\s+([A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ0-9][A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇa-záéíóúàèìòùãõâêîôûç0-9\s\'\-\.&]{1,50}?)(?=[,\.\;\n\r]|\s+(?:para|com|por|através|inscrit|CNPJ))', re.IGNORECASE),
    re.compile(r'apresentação\s+(?:artística|musical)\s+d[ao]\s+(?:dupla\s+|banda\s+|cantor[a]?\s+artística\s+)?([A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ][A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇa-záéíóúàèìòùãõâêîôûç0-9\s&\'\-\.]{2,60}?)(?=[,\.\;\n\r]|\s+(?:para|por|com\s+dur|deten|através|por\s+meio|inscrit))', re.IGNORECASE),
    re.compile(r'Credenciado[a]?:\s+[\d\.]+\s+([A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ][A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇa-záéíóúàèìòùãõâêîôûç\s\-\.]{4,60}?)(?:[,\n\r]|\s+\(|\s+inscrit)', re.IGNORECASE),
]

RE_VALOR = re.compile(r'R\$\s*[\(]?\s*([\d\.]+,\d{2})')
RE_ESTADO_MUN = re.compile(r'ESTADO\s+DE\s+MINAS\s+GERAIS\s*[\r\n]+\s*(?:PREFEITURA|CÂMARA|MUNICÍPIO)\s+(?:MUNICIPAL\s+DE\s+|DE\s+|DO\s+)?([A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ][A-ZÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÇ\s\-]+?)[\r\n]', re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────────────────
# Classe Principal
# ─────────────────────────────────────────────────────────────────────────────

class DiarioOficialExtractor(BaseExtractor):
    async def extract(self) -> list[EventoSchema]:
        log.info("🚀 [v11.4.1] D.O. Extractor — Fusão de Infra e Inteligência Cultural")
        try:
            html = await self.fetch_html("https://www.diariomunicipal.com.br/amm-mg/")
            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            if not pdf_input: return []
            pdf_url = pdf_input.attributes.get("value", "")
            return await self._processar_pdf_streaming(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro crítico: {e}")
            return []

    async def _processar_pdf_streaming(self, pdf_url: str) -> list[EventoSchema]:
        chunks = []
        total_bytes = 0
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            async with client.stream("GET", pdf_url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
                    total_bytes += len(chunk)
                    if total_bytes > MAX_PDF_BYTES: return []
        
        pdf_bytes = b"".join(chunks)
        del chunks
        gc.collect()

        return self._extrair_eventos_streaming(pdf_bytes, pdf_url)

    def _extrair_eventos_streaming(self, pdf_bytes: bytes, pdf_url: str) -> list[EventoSchema]:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_paginas = len(reader.pages)
            log.info(f"📄 Minerando {total_paginas} páginas com Lógica Cultural v2.2")

            eventos: list[EventoSchema] = []
            hashes_vistos = set()
            pbar = tqdm(total=total_paginas, desc="Processando D.O.", unit="pág")

            # Mapeamento de Cidades por Página (v2.2 Heurística)
            mapa_cidades = {}

            # Loop de Processamento
            for i in range(total_paginas):
                pbar.n = i + 1
                pbar.refresh()
                
                texto_pagina = reader.pages[i].extract_text() or ""
                
                # Atualiza Mapa de Cidades se encontrar o cabeçalho do Estado
                for m in RE_ESTADO_MUN.finditer(texto_pagina):
                    nome_cid = m.group(1).strip().title()
                    if len(nome_cid) > 3: mapa_cidades[i] = nome_cid

                # Determina a cidade atual (busca a última cidade mapeada)
                cidade_atual = "Minas Gerais"
                for pag_idx in sorted(mapa_cidades.keys(), reverse=True):
                    if pag_idx <= i:
                        cidade_atual = mapa_cidades[pag_idx]
                        break

                # Divide a página em linhas para aplicar a lógica de Âncora (v2.2)
                linhas = texto_pagina.split('\n')
                for idx, linha in enumerate(linhas):
                    if self._eh_ancora(linha):
                        # Se é âncora, cria um bloco de contexto (janela assimétrica)
                        inicio = max(0, idx - 8)
                        fim = min(len(linhas), idx + 35)
                        bloco = " ".join(linhas[inicio:fim])
                        
                        # Extração Inteligente v2.2
                        artista = self._extrair_artista(bloco)
                        if not artista: continue

                        valor = self._extrair_valor(bloco)
                        tipo = self._classificar_tipo(bloco)
                        
                        # Deduplicação por Hash MD5
                        h = hashlib.md5(f"{artista}{cidade_atual}{tipo}".encode()).hexdigest()[:12]
                        if h not in hashes_vistos:
                            hashes_vistos.add(h)
                            eventos.append(EventoSchema(
                                titulo=f"{tipo}: {artista}",
                                data_evento=datetime.now() + timedelta(days=30),
                                cidade=cidade_atual,
                                local=f"Município de {cidade_atual}",
                                preco_base=valor,
                                fonte=f"AMM-MG (v11.4.1)",
                                url_origem=pdf_url,
                                vibe="show" if "Show" in tipo else "festival"
                            ))

                if i % GC_INTERVALO == 0: gc.collect()

            pbar.close()
            log.info(f"🏁 Concluído: {len(eventos)} eventos culturais reais encontrados.")
            return eventos

        except Exception as e:
            log.error(f"❌ Erro no streaming: {e}")
            return []

    def _eh_ancora(self, linha: str) -> bool:
        l = linha.lower()
        for kw in KEYWORDS_ANCORA:
            if re.search(kw, l):
                for v in VETO_ANCORA:
                    if re.search(v, l): return False
                return True
        return False

    def _extrair_artista(self, bloco: str) -> Optional[str]:
        for padrao in PADROES_ARTISTA:
            m = padrao.search(bloco)
            if m:
                nome = m.group(1).strip()
                # Limpeza de lixo burocrático (v2.2)
                nome = re.split(r'\s+(?:\d{2,}\.|\bCNPJ\b|\bCPF\b|\bLTDA\b|\bMEI\b)', nome, maxsplit=1, flags=re.IGNORECASE)[0]
                nome = re.sub(r'\s+', ' ', nome).strip().rstrip(',.')
                if len(nome) >= 3 and nome.lower() not in BLACKLIST_ARTISTA:
                    return nome.title()
        return None

    def _extrair_valor(self, bloco: str) -> float:
        matches = RE_VALOR.findall(bloco)
        if not matches: return 0.0
        nums = []
        for v in matches:
            try:
                n = float(v.replace(".", "").replace(",", "."))
                if n < 1000000: nums.append(n) # Filtro contra contratos de engenharia
            except: continue
        return max(nums) if nums else 0.0

    def _classificar_tipo(self, bloco: str) -> str:
        b = bloco.lower()
        if "carnaval" in b: return "Show Carnavalesco"
        if "festa junina" in b: return "Festa Junina"
        if "aniversário" in b: return "Aniversário de Cidade"
        if "festival" in b: return "Festival Cultural"
        return "Show Musical"