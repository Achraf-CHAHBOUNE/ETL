import logging
import os
from datetime import datetime

def setup_logging(module_name, data_type=None, log_dir="./logs/Transformer"):
    """
    Set up logging to write to a daily file in a data-type and module-specific subfolder and to the console.
    
    Args:
        module_name (str): Name of the module (e.g., 'Main', 'Transformer', 'Tools').
        data_type (str, optional): Data type (e.g., '5min', '15min', 'mgw'). If None, logs go to log_dir/module_name/.
        log_dir (str): Base directory where data-type and module-specific log subfolders will be stored.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create data-type-specific log subfolder (e.g., ./logs/Transformer/5min/Transformer/)
    if data_type:
        module_log_dir = os.path.join(log_dir, data_type, module_name)
        logger_name = f"{data_type}.{module_name}"  # Unique logger name per thread (e.g., 5min.Transformer)
    else:
        module_log_dir = os.path.join(log_dir, module_name)
        logger_name = module_name

    if not os.path.exists(module_log_dir):
        os.makedirs(module_log_dir)
    
    # Daily log filename (e.g., transformer_20250428.log)
    date_str = datetime.now().strftime("%Y%m%d")
    log_filename = f"{module_name.lower()}_{date_str}.log"
    
    # Full path for the log file
    log_path = os.path.join(module_log_dir, log_filename)
    
    # Create a logger with a unique name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to avoid duplicate logs
    logger.handlers.clear()
    
    # Create file handler (append mode for thread safety)
    file_handler = logging.FileHandler(log_path, mode='a')
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Define log format
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log initialization (only log this if the file is new or empty)
    if not os.path.exists(log_path) or os.path.getsize(log_path) == 0:
        logger.info(f"Logging initialized for {logger_name}. Logs are being written to: {log_path}")
    
    return logger