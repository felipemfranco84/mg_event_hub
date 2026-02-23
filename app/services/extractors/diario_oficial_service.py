"""
Padrão de Qualidade: Extrator de Elite v11.5.0 (Baseado integralmente na v2.2 FINAL).
Motivo: Eliminação total de lógicas anteriores para garantir recall e precisão idênticos 
aos testes manuais, mantendo a compatibilidade com a infraestrutura de 1GB RAM.
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
# PARÂMETROS E CONSTANTES (IDÊNTICOS À v2.2)
# ─────────────────────────────────────────────────────────────────────────────

JANELA_FRENTE = 35
JANELA_ATRAS  = 8
JANELA_VALOR  = 18

KEYWORDS_ANCORA = [
    r"show\s+musical", r"show\s+artístico", r"apresentação\s+artística", 
    r"apresentação\s+musical", r"serviços\s+artísticos", r"calendário\s+cultural",
    r"credenciamento.*(?:artista|banda|músic)", r"grupo\s+musical",
    r"festival\s+(?:gastronômico|cultural|de\s+\w)", r"festa\s+(?:junina|municipal|de\s+agosto)",
    r"aniversário.*(?:cidade|município)", r"aulas?\s+de\s+(?:zumba|dança|karatê|atletismo|música|canto)",
    r"espetáculo\s+(?:artístico|cultural|teatral)", r"contratação.*artista",
    r"contratação\s+da\s+(?:banda|dupla)", r"festividades.*carnaval",
    r"banda\s+musical\s+composta", r"artistas?\s+musicais?",
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

# REGEX DE EXTRAÇÃO (100% FIÉIS À v2.2)
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
# CLASSE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class DiarioOficialExtractor(BaseExtractor):
    BASE_URL = "https://www.diariomunicipal.com.br/amm-mg/"

    async def extract(self) -> list[EventoSchema]:
        log.info("🚀 [v11.5.0] D.O. Extractor — Iniciando Cópia Fiel da Lógica v2.2")
        try:
            html = await self.fetch_html(self.BASE_URL)
            tree = HTMLParser(html)
            pdf_url = tree.css_first("input#urlPdf").attributes.get("value", "")
            return await self._processar_pdf_streaming(pdf_url)
        except Exception as e:
            log.error(f"❌ Erro crítico: {e}")
            return []

    async def _processar_pdf_streaming(self, pdf_url: str) -> list[EventoSchema]:
        chunks = []
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            async with client.stream("GET", pdf_url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
        
        pdf_bytes = b"".join(chunks)
        gc.collect()
        return self._extrair_eventos_fiel(pdf_bytes, pdf_url)

    def _extrair_eventos_fiel(self, pdf_bytes: bytes, pdf_url: str) -> list[EventoSchema]:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            total_paginas = len(reader.pages)
            log.info(f"📄 Minerando {total_paginas} páginas...")

            eventos: list[EventoSchema] = []
            hashes_vistos = set()
            pbar = tqdm(total=total_paginas, desc="Executando v2.2", unit="pág")

            mapa_cidades = {}
            # Pré-mapeamento de cidades (idêntico à lógica v2.2)
            for i in range(total_paginas):
                texto = reader.pages[i].extract_text() or ""
                for m in RE_ESTADO_MUN.finditer(texto):
                    mapa_cidades[i] = m.group(1).strip().title()

            for i in range(total_paginas):
                pbar.n = i + 1
                pbar.refresh()
                
                texto_pagina = reader.pages[i].extract_text() or ""
                
                # Resolução de cidade por página
                cidade_atual = "Minas Gerais"
                for p_idx in sorted(mapa_cidades.keys(), reverse=True):
                    if p_idx <= i:
                        cidade_atual = mapa_cidades[p_idx]
                        break

                linhas = texto_pagina.split('\n')
                for idx, linha in enumerate(linhas):
                    # 1. Verifica Âncora
                    eh_ancora = False
                    l_low = linha.lower()
                    for kw in KEYWORDS_ANCORA:
                        if re.search(kw, l_low):
                            eh_ancora = True
                            for v in VETO_ANCORA:
                                if re.search(v, l_low): 
                                    eh_ancora = False
                                    break
                            break
                    
                    if not eh_ancora:
                        continue

                    # 2. Captura Janela Assimétrica
                    inicio = max(0, idx - JANELA_ATRAS)
                    fim = min(len(linhas), idx + JANELA_FRENTE)
                    bloco_completo = " ".join(linhas[inicio:fim])
                    
                    # 3. Extração de Artista (v2.2)
                    artista = None
                    for padrao in PADROES_ARTISTA:
                        m = padrao.search(bloco_completo)
                        if m:
                            nome = m.group(1).strip()
                            nome = re.split(r'\s+(?:\d{2,}\.|\bCNPJ\b|\bCPF\b|\bLTDA\b|\bMEI\b)', nome, maxsplit=1, flags=re.IGNORECASE)[0]
                            nome = re.sub(r'\s+', ' ', nome).strip().rstrip(',.')
                            if len(nome) >= 3 and nome.lower() not in BLACKLIST_ARTISTA:
                                artista = nome.title()
                                break
                    
                    if not artista: continue

                    # 4. Extração de Valor (Janela v2.2)
                    inicio_val = max(0, idx - 5)
                    fim_val = min(len(linhas), idx + JANELA_VALOR)
                    bloco_valor = " ".join(linhas[inicio_val:fim_val])
                    
                    valor = 0.0
                    matches_v = RE_VALOR.findall(bloco_valor)
                    if matches_v:
                        nums = []
                        for v in matches_v:
                            try:
                                n = float(v.replace(".", "").replace(",", "."))
                                if n < 1000000: nums.append(n)
                            except: continue
                        valor = max(nums) if nums else 0.0

                    tipo = self._classificar_tipo(bloco_completo)
                    
                    h = hashlib.md5(f"{artista}{cidade_atual}{tipo}{valor}".encode()).hexdigest()[:12]
                    if h not in hashes_vistos:
                        hashes_vistos.add(h)
                        eventos.append(EventoSchema(
                            titulo=f"{tipo}: {artista}",
                            data_evento=datetime.now() + timedelta(days=30),
                            cidade=cidade_atual,
                            local=f"Município de {cidade_atual}",
                            preco_base=valor,
                            fonte="AMM-MG (v11.5.0)",
                            url_origem=pdf_url,
                            vibe="show"
                        ))

                if i % 30 == 0: gc.collect()

            pbar.close()
            log.info(f"✅ Finalizado! Minerados {len(eventos)} eventos com recall máximo.")
            return eventos
        except Exception as e:
            log.error(f"❌ Falha: {e}")
            return []

    def _classificar_tipo(self, bloco: str) -> str:
        b = bloco.lower()
        if "carnaval" in b: return "Show Carnavalesco"
        if "festa junina" in b: return "Festa Junina"
        if "aniversário" in b: return "Aniversário de Cidade"
        if "festival" in b: return "Festival Cultural"
        if re.search(r"aulas?\s+de", b): return "Aula/Atividade Cultural"
        return "Show Musical"