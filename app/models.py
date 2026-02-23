"""
Padrão de Qualidade: SQLAlchemy 2.0 Mapping.
Motivo: Mapear a tabela de eventos no SQLite de forma otimizada.
"""
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from sqlalchemy import String, Float, DateTime, func
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
    preco_base: Mapped[float] = mapped_column(Float, default=0.0)
    fonte: Mapped[str] = mapped_column(String(50), nullable=False)
    url_origem: Mapped[str] = mapped_column(String(500), nullable=False)
    vibe: Mapped[str] = mapped_column(String(50), nullable=False)
    detectado_em: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<EventoModel(titulo={self.titulo}, cidade={self.cidade})>"