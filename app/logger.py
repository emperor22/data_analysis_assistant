import sys
from loguru import logger
from app.config import Config


def setup_logger():
    logger.remove()

    log_format = "<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

    logger.add(
        sys.stderr, colorize=True, format=log_format, level=Config.LOG_LEVEL_STDERR
    )
    logger.add(
        "logs.log",
        level=Config.LOG_LEVEL_fILE,
        format=log_format,
        colorize=False,
        backtrace=True,
        diagnose=True,
    )


setup_logger()
