"""
Padrão de Qualidade: Query Filtering & Performance.
Motivo: Permitir busca segmentada por cidade e categoria.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.core.database import get_db
from app.models import EventoModel
from app.core.logger import log
from typing import Optional

router = APIRouter(prefix="/eventos", tags=["Eventos"])

@router.get("/")
async def listar_eventos(
    cidade: Optional[str] = Query(None, description="Filtrar por nome da cidade"),
    vibe: Optional[str] = Query(None, description="Filtrar por categoria (show, pousada)"),
    db: AsyncSession = Depends(get_db)
):
    """Retorna eventos com filtros opcionais."""
    try:
        query = select(EventoModel)
        
        if cidade:
            # Filtro case-insensitive parcial (LIKE)
            query = query.where(EventoModel.cidade.ilike(f"%{cidade}%"))
        
        if vibe:
            query = query.where(EventoModel.vibe == vibe)
            
        # Ordenar pelos eventos mais próximos
        query = query.order_by(EventoModel.data_evento.asc())
        
        result = await db.execute(query)
        eventos = result.scalars().all()
        
        return {
            "total": len(eventos),
            "filtros": {"cidade": cidade, "vibe": vibe},
            "data": eventos
        }
    except Exception as e:
        log.error(f"Erro ao listar eventos com filtros: {e}")
        return {"error": "Falha ao recuperar eventos"}, 500
