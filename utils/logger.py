import logging

def get_logger(name:str):
    logger = logging.getLogger(name)
    logger.setLevel(level=logging.INFO)
    
    if not logger.handlers: 
        handler = logging.StreamHandler()
        handler.setLevel(level=logging.INFO)
        
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger