import sys
import json
from loguru import logger



def setup_logger():
    logger.remove()
    
    log_format = "<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    
    log_level_stderr = 'DEBUG'
    log_level_file = 'DEBUG'
    
    logger.add(sys.stderr, colorize=True, format=log_format, level=log_level_stderr)
    logger.add("logs.log", level=log_level_file, format=log_format, colorize=False, backtrace=True, diagnose=True)

setup_logger()
