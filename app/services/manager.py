import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.logger import log

from app.services.extractors.portal_bh_service import PortalBHExtractor
from app.services.extractors.sympla_service import SymplaExtractor
from app.services.extractors.palacio_artes_service import PalacioArtesExtractor
from app.services.extractors.diario_amm_service import DiarioAMMExtractor

class EventManager:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.scrapers = [
            PortalBHExtractor(),
            SymplaExtractor(),
            PalacioArtesExtractor(),
            DiarioAMMExtractor()
        ]

    async def _aplicar_migrations(self):
        try:
            await self.session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_eventos_id_unico ON eventos(id_unico)"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN imagem_url VARCHAR(500)"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN descricao TEXT"))
            await self.session.execute(text("ALTER TABLE eventos ADD COLUMN categoria VARCHAR(100)"))
            await self.session.commit()
        except Exception:
            await self.session.rollback()

    async def run_all_scrapers(self):
        log.info(f"🚀 Iniciando orquestrador v4.0 com {len(self.scrapers)} fontes...")
        await self._aplicar_migrations()
        todos_eventos = []
        relatorio = {}

        sql = text("""
            INSERT OR IGNORE INTO eventos 
            (id_unico, titulo, data_evento, cidade, local, descricao, categoria, preco_base, url_evento, imagem_url, fonte)
            VALUES 
            (:id_unico, :titulo, :data_evento, :cidade, :local, :descricao, :categoria, :preco_base, :url_evento, :imagem_url, :fonte)
        """)

        for scraper in self.scrapers:
            nome = scraper.__class__.__name__
            try:
                log.info(f"📡 Iniciando: {nome}")
                eventos = await scraper.extract()
                
                if not eventos:
                    relatorio[nome] = {"capturados": 0, "persistidos": 0, "erros": 0}
                    log.warning(f"⚠️ {nome}: 0 eventos.")
                    continue
                
                count_persistidos = 0
                count_erros = 0
                
                for ev in eventos:
                    try:
                        res = await self.session.execute(sql, ev.model_dump())
                        if res.rowcount > 0:
                            count_persistidos += 1
                    except Exception as e:
                        count_erros += 1
                        log.debug(f"Erro BD ({ev.titulo}): {e}")
                
                await self.session.commit()
                relatorio[nome] = {"capturados": len(eventos), "persistidos": count_persistidos, "erros": count_erros}
                
                log.info(f"[Manager] {nome}: {len(eventos)} capturados | {count_persistidos} novos | {count_erros} erros")
                todos_eventos.extend(eventos)
                
            except Exception as e:
                log.error(f"❌ Falha no motor {nome}: {e}")
                await self.session.rollback()

        total_cap = sum(r.get("capturados", 0) for r in relatorio.values())
        total_pers = sum(r.get("persistidos", 0) for r in relatorio.values())
        log.info(f"✨ CICLO COMPLETO: {total_cap} capturados | {total_pers} novos no banco")
        return todos_eventos

DataManager = EventManager
