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
