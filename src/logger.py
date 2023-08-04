import logging
from typing import Optional

def get_console_logger(
        # name: Optional[str] = 'tutorial'
    ) -> logging.Logger:
    
    # # Create logger if it doesn't exist
    logger = logging.getLogger('feature_pipeline')
    logger.setLevel(logging.DEBUG)

    return logger