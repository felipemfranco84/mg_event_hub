#!/bin/bash
echo "🚀 Iniciando Atualização v15.0.0 - MG Event Hub..."

# 1. Dependências do novo motor
pip install pdfplumber > /dev/null 2>&1

# 2. Reset do Banco de Dados Obsoleto
rm -f ./data/mg_events.db
echo "🗑️ Banco de dados antigo removido para atualização de Schema."

# 3. Recriando app/models.py
cat << 'FILE_EOF' > app/models.py
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from sqlalchemy import String, Float, DateTime, func, Text
from datetime import datetime

class Base(DeclarativeBase):
    pass

class EventoModel(Base):
    __tablename__ = "eventos"
    id_unico: Mapped[str] = mapped_column(String(64), primary_key=True)
    titulo: Mapped[str] = mapped_column(String(255), nullable=False)
    data_evento: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    cidade: Mapped[str] = mapped_column(String(100), nullable=False)
    local: Mapped[str] = mapped_column(String(255), nullable=False)
    descricao: Mapped[str] = mapped_column(Text, nullable=True)
    categoria: Mapped[str] = mapped_column(String(100), nullable=True)
    preco_base: Mapped[float] = mapped_column(Float, default=0.0)
    url_evento: Mapped[str] = mapped_column(String(500), nullable=False)
    imagem_url: Mapped[str] = mapped_column(String(500), nullable=True)
    fonte: Mapped[str] = mapped_column(String(100), nullable=False)
    detectado_em: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
FILE_EOF

# 4. Recriando app/schemas/evento.py
cat << 'FILE_EOF' > app/schemas/evento.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class EventoSchema(BaseModel):
    id_unico: str
    titulo: str
    data_evento: datetime
    cidade: str
    local: str
    descricao: Optional[str] = ""
    categoria: str
    preco_base: float
    url_evento: str
    imagem_url: Optional[str] = ""
    fonte: str
FILE_EOF

# 5. Recriando app/services/manager.py (O Motor Upsert)
cat << 'FILE_EOF' > app/services/manager.py
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.logger import log

from app.services.extractors.portal_bh_service import PortalBHExtractor
from app.services.extractors.sympla_service import SymplaExtractor

class EventManager:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.scrapers = [
            PortalBHExtractor(),
            SymplaExtractor()
        ]

    async def run_all_scrapers(self):
        log.info(f"🚀 Iniciando ciclo UPSERT com {len(self.scrapers)} fontes...")
        todos_eventos = []

        sql = text("""
            INSERT INTO eventos (id_unico, titulo, data_evento, cidade, local, descricao, categoria, preco_base, url_evento, imagem_url, fonte)
            VALUES (:id_unico, :titulo, :data_evento, :cidade, :local, :descricao, :categoria, :preco_base, :url_evento, :imagem_url, :fonte)
            ON CONFLICT(id_unico) DO UPDATE SET
                titulo      = excluded.titulo,
                data_evento = excluded.data_evento,
                local       = excluded.local,
                descricao   = excluded.descricao,
                categoria   = excluded.categoria,
                preco_base  = excluded.preco_base,
                url_evento  = excluded.url_evento,
                imagem_url  = excluded.imagem_url,
                fonte       = excluded.fonte
        """)

        for scraper in self.scrapers:
            try:
                eventos = await scraper.extract()
                if not eventos: continue
                
                count = 0
                for ev in eventos:
                    try:
                        await self.session.execute(sql, ev.model_dump())
                        count += 1
                    except Exception as e:
                        log.debug(f"Erro em {ev.titulo}: {e}")
                
                await self.session.commit()
                log.info(f"💾 {scraper.__class__.__name__}: {count} eventos consolidados no banco.")
                todos_eventos.extend(eventos)
            except Exception as e:
                log.error(f"❌ Falha no motor {scraper.__class__.__name__}: {e}")
                await self.session.rollback()

        return todos_eventos

DataManager = EventManager
FILE_EOF

# 6. Recriando o Portal BH Seguro (v3.0 adaptado)
cat << 'FILE_EOF' > app/services/extractors/portal_bh_service.py
import hashlib, re, httpx, asyncio
from datetime import datetime
from selectolax.parser import HTMLParser
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class PortalBHExtractor(BaseExtractor):
    URL = "https://portalbelohorizonte.com.br/eventos"
    
    async def extract(self) -> list[EventoSchema]:
        eventos = []
        headers = {"User-Agent": "Mozilla/5.0"}
        
        try:
            async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                resp = await client.get(self.URL)
                tree = HTMLParser(resp.text)
                
                for a in tree.css("article.evento, div.card-evento"):
                    titulo_node = a.css_first("h3") or a.css_first("h2")
                    if not titulo_node: continue
                    
                    titulo = titulo_node.text(strip=True)
                    if len(titulo) < 5 or "whatsapp" in titulo.lower(): continue
                    
                    link = a.css_first("a")
                    url = link.attributes.get("href", "") if link else ""
                    url_full = url if url.startswith("http") else f"https://portalbelohorizonte.com.br{url}"
                    
                    id_unico = hashlib.md5(f"{titulo}-{url_full}".encode()).hexdigest()
                    
                    eventos.append(EventoSchema(
                        id_unico=id_unico,
                        titulo=titulo.upper(),
                        data_evento=datetime.now(),
                        cidade="Belo Horizonte",
                        local="Consulte a fonte",
                        descricao="Evento extraído via Portal BH",
                        categoria="Cultura / Entretenimento",
                        preco_base=0.0,
                        url_evento=url_full,
                        imagem_url="",
                        fonte="Portal BH (v3.0)"
                    ))
        except Exception as e:
            log.error(f"❌ Erro Portal BH: {e}")
            
        return eventos
FILE_EOF

# 7. Criando Sympla Extractor (v3.0 - JSON-LD)
cat << 'FILE_EOF' > app/services/extractors/sympla_service.py
import hashlib, httpx, re, json, asyncio
from datetime import datetime
from app.schemas.evento import EventoSchema
from app.services.extractors.base import BaseExtractor
from app.core.logger import log

class SymplaExtractor(BaseExtractor):
    URL = "https://www.sympla.com.br/eventos/belo-horizonte-mg"
    
    async def extract(self) -> list[EventoSchema]:
        eventos = []
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        try:
            async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                resp = await client.get(self.URL)
                pattern = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
                
                for match in pattern.finditer(resp.text):
                    try:
                        data = json.loads(match.group(1))
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            if "Event" not in item.get("@type", ""): continue
                            
                            titulo = item.get("name", "").strip()
                            loc = item.get("location", {})
                            local = loc.get("name", "Belo Horizonte") if isinstance(loc, dict) else str(loc)
                            url_ev = item.get("url", "")
                            
                            id_unico = hashlib.md5(url_ev.encode()).hexdigest()
                            
                            eventos.append(EventoSchema(
                                id_unico=id_unico,
                                titulo=titulo,
                                data_evento=datetime.now(),
                                cidade="Belo Horizonte",
                                local=local,
                                descricao="",
                                categoria="Diversos",
                                preco_base=0.0,
                                url_evento=url_ev,
                                imagem_url=item.get("image", ""),
                                fonte="Sympla (v3.0)"
                            ))
                    except: continue
        except Exception as e:
            log.error(f"❌ Erro Sympla: {e}")
            
        return eventos
FILE_EOF

# 8. Criando o Exportador JSON estático (Frontend Desacoplado)
cat << 'FILE_EOF' > exportar_json.py
import sqlite3, json, os, sys
from datetime import datetime

def exportar():
    DB_PATH = "./data/mg_events.db"
    OUT_PATH = "./data/eventos.json"
    
    if not os.path.exists(DB_PATH):
        print("Banco não encontrado.")
        return
        
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM eventos ORDER BY data_evento ASC")
    
    eventos = [dict(row) for row in cursor.fetchall()]
    payload = {"gerado_em": datetime.now().isoformat(), "total": len(eventos), "eventos": eventos}
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"✅ {len(eventos)} eventos exportados para {OUT_PATH}")

if __name__ == "__main__":
    exportar()
FILE_EOF

# 9. Novo index.html Reativo (Consome o JSON gerado)
cat << 'FILE_EOF' > app/templates/index.html
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>MG Event Hub — Agenda Cultural</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50 min-h-screen p-6 font-sans">
  <div class="max-w-5xl mx-auto">
    <h1 class="text-3xl font-black text-gray-900 mb-8">MINAS EVENTOS ⚡</h1>
    <div id="grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"></div>
  </div>

  <script>
    async function carregar() {
      try {
        const res = await fetch('/data/eventos.json');
        const data = await res.json();
        const grid = document.getElementById('grid');
        
        grid.innerHTML = data.eventos.map(e => `
          <div class="bg-white rounded-xl p-5 shadow-sm border border-gray-100 hover:shadow-md transition">
            <span class="text-xs font-bold text-blue-600 uppercase bg-blue-50 px-2 py-1 rounded">${e.fonte}</span>
            <h3 class="font-bold text-gray-800 mt-3 text-lg leading-tight">${e.titulo}</h3>
            <p class="text-gray-500 text-sm mt-2">📍 ${e.local}</p>
            <a href="${e.url_evento}" target="_blank" class="mt-4 block w-full text-center bg-gray-900 text-white py-2 rounded-lg font-medium text-sm">Ver Detalhes</a>
          </div>
        `).join('');
      } catch(e) {
        document.getElementById('grid').innerHTML = '<p class="text-red-500">Gere o JSON primeiro com python exportar_json.py</p>';
      }
    }
    carregar();
  </script>
</body>
</html>
FILE_EOF

echo "✅ Ambiente v15.0.0 configurado com sucesso!"
