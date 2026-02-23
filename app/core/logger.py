"""
Padrão de Qualidade: Logging Estruturado.
Motivo: Corrigir exportação do objeto log para uso global.
"""
import sys
from loguru import logger
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def setup_logger():
    logger.remove()
    
    # Console
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

    # Arquivo
    logger.add(
        LOG_DIR / "app.log",
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
    )
    return logger

# AQUI ESTÁ A CHAVE: Exportando como 'log'
log = setup_logger()
