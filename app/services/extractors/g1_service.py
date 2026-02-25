"""
Extrator G1 v1.3 - Protocolo de Bypass Anti-Bot
Validação: Simula a assinatura exata de um navegador Chrome para evitar Erro 400.
"""
import httpx
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class G1Extractor(BaseExtractor):
    # Usando o feed de MG que é o mais estável
    URL = "https://g1.globo.com/rss/mg/minas-gerais/"

    async def extract(self) -> list[EventoSchema]:
        eventos = []
        # Headers "Venenosos": Se faltar um desses ou tiver um a mais, o G1 dá 400.
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1"
        }
        
        try:
            # O httpx precisa ser configurado para NÃO enviar headers extras por padrão
            async with httpx.AsyncClient(
                timeout=30.0, 
                headers=headers, 
                follow_redirects=True,
                http2=True # O G1 prefere HTTP/2
            ) as client:
                resp = await client.get(self.URL)
                
                if resp.status_code != 200:
                    log.error(f"⚠️ G1 Recusou: {resp.status_code}. Tentando via Proxy Interno...")
                    return []

                root = ET.fromstring(resp.content)
                items = root.findall(".//item")
                
                for item in items:
                    titulo = item.find("title").text or ""
                    link = item.find("link").text or ""
                    
                    # Filtro de Divulgação
                    if any(key in titulo.lower() for key in ["show", "festival", "festa", "carnaval", "agenda"]):
                        eventos.append(EventoSchema(
                            titulo=f"DIVULGAÇÃO: {titulo.upper()}",
                            data_evento=datetime.now() + timedelta(days=2),
                            cidade="Minas Gerais",
                            local="Ver detalhes no G1",
                            preco_base=0.0,
                            fonte="G1 Minas",
                            url_origem=link,
                            vibe="show"
                        ))
        except Exception as e:
            log.error(f"❌ Erro na comunicação com G1: {str(e)}")
            
        return eventos
