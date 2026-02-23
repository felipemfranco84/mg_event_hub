"""
Padrão de Qualidade: Multi-Source Coordination.
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.sqlite import insert
from app.models import EventoModel
from app.services.extractors.sympla_service import SymplaExtractor
from app.services.extractors.diario_oficial_service import DiarioOficialExtractor
from app.services.extractors.hospedagem_service import HospedagemExtractor
from app.core.logger import log

class DataManager:
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        # GARANTIA: Os 3 scrapers devem estar aqui
        self.scrapers = [
            SymplaExtractor(),
            DiarioOficialExtractor(),
            HospedagemExtractor()
        ]

    async def run_all_scrapers(self):
        log.info(f"Iniciando ciclo de extração completo com {len(self.scrapers)} scrapers...")
        total_novos = 0
        for scraper in self.scrapers:
            try:
                eventos = await scraper.extract()
                if eventos:
                    count = await self.save_to_db(eventos)
                    total_novos += count
            except Exception as e:
                log.error(f"Falha no scraper {type(scraper).__name__}: {e}")
        
        await self.db.commit()
        return total_novos

    async def save_to_db(self, eventos):
        new_count = 0
        for evento in eventos:
            try:
                dados = evento.model_dump()
                stmt = insert(EventoModel).values(**dados)
                stmt = stmt.on_conflict_do_nothing(index_elements=['id_unico'])
                result = await self.db.execute(stmt)
                if result.rowcount > 0:
                    new_count += 1
            except Exception as e:
                log.error(f"Erro ao persistir {evento.titulo}: {e}")
        return new_count
