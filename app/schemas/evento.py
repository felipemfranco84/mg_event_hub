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
