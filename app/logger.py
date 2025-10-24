import sys
import json
from loguru import logger



def setup_logger():
    logger.remove()
    logger.add(sys.stderr, colorize=True, format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

setup_logger()
