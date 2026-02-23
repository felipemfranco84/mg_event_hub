"""
Padrão de Qualidade: Advanced NLP Pipeline & Heuristics.
Motivo: Extração estruturada de shows em PDFs não padronizados, isolando
contexto geográfico e aplicando múltiplas camadas de reconhecimento (Regex).
"""
import re
import io
import httpx
import pdfplumber
from datetime import datetime, timedelta
from app.services.extractors.base import BaseExtractor
from app.schemas.evento import EventoSchema
from app.core.logger import log
from selectolax.parser import HTMLParser

class DiarioOficialExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.diariomunicipal.com.br/amm-mg/"
        
        # Dicionário para conversão de datas textuais em português
        self.meses_pt = {
            "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
            "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
            "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
        }

        # Padrões compilados para performance (Regex Hub)
        self.re_gatilhos = re.compile(r"(contratação|show|apresentação|inexigibilidade|festa)", re.IGNORECASE)
        self.re_cidade = re.compile(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s\-]+)", re.IGNORECASE)
        
        # Padrões de extração de entidades
        self.re_artista = re.compile(r"(?:DUPLA|BANDA|SHOW(?: ARTÍSTICO)?(?: DE)?|CONTRATAÇÃO (?:DA|DO|DE)|ARTISTA)\s+([A-ZÀ-Ú0-9\s&\'\-]+?)(?:\s+PARA|\s+NO DIA|,|\s+DURANTE|\.)", re.IGNORECASE)
        self.re_data_numerica = re.compile(r"(\d{2}/\d{2}/\d{4})")
        self.re_data_textual = re.compile(r"DIA\s+(\d{1,2})\s+DE\s+([A-ZÀ-Ú]+)\s+DE\s+(\d{4})", re.IGNORECASE)
        self.re_valor = re.compile(r"R\$\s?([\d\.]+,\d{2})")
        self.re_evento = re.compile(r"(?:FESTA DE|CARNAVAL|ANIVERSÁRIO|EXPO|FESTIVAL)\s+([A-ZÀ-Ú0-9\s]+?)(?:\.|,|NO MUNICÍPIO)", re.IGNORECASE)

    async def extract(self):
        """Orquestra o download e a extração do PDF da edição atual."""
        log.info("🚀 Iniciando Pipeline NLP no Diário Oficial (AMM-MG)")
        
        try:
            html = await self.fetch_html(self.base_url)
            if not html: return []

            tree = HTMLParser(html)
            pdf_input = tree.css_first("input#urlPdf")
            
            if not pdf_input:
                log.warning("⚠️ Link do PDF não encontrado via seletor input#urlPdf.")
                return []

            pdf_url = pdf_input.attributes.get("value")
            log.info(f"📄 Baixando PDF para memória: {pdf_url[:70]}...")

            return await self._processar_pdf(pdf_url)

        except Exception as e:
            log.error(f"❌ Falha crítica na orquestração do D.O.: {e}")
            return []

    async def _processar_pdf(self, pdf_url: str):
        """Faz o download do PDF e aplica a extração de texto contínua via pdfplumber."""
        eventos_validados = []
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=90.0) as client:
            try:
                resp = await client.get(pdf_url)
                resp.raise_for_status()
                
                # Carrega o PDF na memória (sem tocar no disco)
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    texto_completo = ""
                    log.info(f"📚 PDF aberto com sucesso. Lendo {len(pdf.pages)} páginas...")
                    
                    for pagina in pdf.pages:
                        texto = pagina.extract_text()
                        if texto:
                            texto_completo += texto + "\n"
                            
                log.info("🔍 Iniciando chunking geográfico e caça aos gatilhos...")
                eventos_validados = self._analisar_texto_completo(texto_completo, pdf_url)
                
                log.info(f"✅ Extração finalizada. {len(eventos_validados)} contratos estruturados identificados.")
                return eventos_validados

            except Exception as e:
                log.error(f"❌ Erro ao ler conteúdo do PDF com pdfplumber: {e}")
                return []

    def _analisar_texto_completo(self, texto_completo: str, url: str):
        """Divide o texto por prefeituras e busca os padrões de shows."""
        encontrados = []
        
        # Divide o texto gigantesco em blocos, usando as Prefeituras como delimitadores
        blocos = self.re_cidade.split(texto_completo)
        
        # O split retorna [Lixo_antes, Cidade_1, Bloco_1, Cidade_2, Bloco_2...]
        # Iteramos de 2 em 2 para pegar o par (Cidade, Conteúdo)
        for i in range(1, len(blocos) - 1, 2):
            cidade = blocos[i].strip().upper()
            conteudo = blocos[i+1]
            
            # Filtro de Relevância: O bloco pertence a um contexto de evento?
            if self.re_gatilhos.search(conteudo):
                try:
                    evento = self._extrair_entidades(cidade, conteudo, url)
                    if evento:
                        encontrados.append(evento)
                except Exception as e:
                    log.debug(f"⚠️ Erro ao processar bloco da cidade {cidade}: {e}")
                    continue
                    
        return encontrados

    def _extrair_entidades(self, cidade: str, texto_bloco: str, url: str):
        """Aplica as regexes para isolar Artista, Data, Valor e Nome do Evento."""
        # Limpa quebras de linha que atrapalham a Regex
        texto_limpo = re.sub(r'\s+', ' ', texto_bloco)
        
        # 1. Identificar o Artista
        match_artista = self.re_artista.search(texto_limpo)
        if not match_artista:
            return None # Se não achou artista claro, descarta para evitar falsos positivos
            
        artista = match_artista.group(1).strip()
        
        # Higienização: evitar que pegue textos longos demais por falha de regex
        if len(artista) > 60 or "EXTRATO" in artista or "PROCESSO" in artista:
            return None
            
        # 2. Identificar Valor
        match_valor = self.re_valor.search(texto_limpo)
        preco = 0.0
        if match_valor:
            preco_str = match_valor.group(1).replace(".", "").replace(",", ".")
            preco = float(preco_str)
            
        # 3. Identificar Evento
        match_evento = self.re_evento.search(texto_limpo)
        nome_evento = match_evento.group(1).strip() if match_evento else "Evento Municipal"
        
        # 4. Identificar Data
        data_evento = self._extrair_data(texto_limpo)
        
        log.info(f"🎯 SUCESSO | Cidade: {cidade} | Artista: {artista} | Valor: R${preco}")

        return EventoSchema(
            titulo=f"SHOW: {artista} ({nome_evento})",
            data_evento=data_evento,
            cidade=cidade,
            local="Verificar no Edital (Praça Pública)",
            preco_base=preco,
            fonte="amm_mg_pdf",
            url_origem=url,
            vibe="show"
        )

    def _extrair_data(self, texto: str) -> datetime:
        """Tenta encontrar data numérica (13/02/2026) ou textual (09 de maio de 2026)."""
        # Tenta data textual primeiro
        match_texto = self.re_data_textual.search(texto)
        if match_texto:
            try:
                dia = int(match_texto.group(1))
                mes_str = match_texto.group(2).lower()
                ano = int(match_texto.group(3))
                mes = self.meses_pt.get(mes_str, 1) # Default 1 se falhar
                dt = datetime(ano, mes, dia)
                if dt > datetime.now():
                    return dt
            except ValueError:
                pass

        # Fallback para data numérica
        match_num = self.re_data_numerica.search(texto)
        if match_num:
            try:
                dt = datetime.strptime(match_num.group(1), "%d/%m/%Y")
                if dt > datetime.now():
                    return dt
            except ValueError:
                pass
                
        # Fallback de segurança: 45 dias no futuro se não achar nada claro
        return datetime.now() + timedelta(days=45)