import logging
import sys
def get_logger(name=None):
    """
    Returns a configured logger instance.
    
    Args:
        name (str, optional): The name of the logger. Defaults to the module name.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Get logger with the provided name or the caller's module name
    logger = logging.getLogger(name)
    
    # Only configure handlers if they haven't been added yet
    if not logger.handlers:
        # Set log level
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add formatter to handler
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        # Prevent propagation to root logger to avoid duplicate logs
        logger.propagate = False
    
    return logger