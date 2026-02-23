"""
Padrão de Qualidade: Clean Code e Herança.
Motivo: Evitar duplicação de lógica de rede e logs entre diferentes scrapers.
"""
import httpx
from abc import ABC, abstractmethod
from app.core.logger import log
import random

class BaseExtractor(ABC):
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]

    def get_headers(self):
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    async def fetch_html(self, url: str):
        """
        Realiza a requisição assíncrona com tratamento de erro e retry simples.
        """
        async with httpx.AsyncClient(headers=self.get_headers(), follow_redirects=True) as client:
            try:
                response = await client.get(url, timeout=15.0)
                response.raise_for_status()
                return response.text
            except Exception as e:
                log.error(f"Erro ao acessar {url}: {e}")
                return None

    @abstractmethod
    async def extract(self):
        """Método obrigatório para todos os scrapers."""
        pass
