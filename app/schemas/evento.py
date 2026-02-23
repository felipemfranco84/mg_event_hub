"""
Padrão de Qualidade: Identidade Única Determinística.
"""
from pydantic import BaseModel, Field, HttpUrl, model_validator
from datetime import datetime
from typing import Optional
import hashlib

class EventoSchema(BaseModel):
    titulo: str
    data_evento: datetime
    cidade: str
    local: str
    preco_base: float = 0.0
    fonte: str
    url_origem: str  # Mudamos para str para facilitar o SQLite
    vibe: str
    detectado_em: datetime = Field(default_factory=datetime.now)
    id_unico: Optional[str] = None

    @model_validator(mode='after')
    def gerar_id_unico(self) -> 'EventoSchema':
        if not self.id_unico:
            # Gera hash baseado em título, data e cidade para evitar duplicatas
            seed = f"{self.titulo}{self.data_evento}{self.cidade}".lower()
            self.id_unico = hashlib.sha256(seed.encode()).hexdigest()
        return self
