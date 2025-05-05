import threading
from utils.logger import setup_logging  # Updated import
from utils.transformer import Transformer
from utils.config import CONFIGS

def run_transformer(config, data_type):
    """
    Main function to run the transformer for a specific data type.
    
    Args:
        config (dict): Configuration dictionary for the data type.
        data_type (str): Type of data being processed (e.g., '5min', '15min', 'mgw').
    """
    # Initialize logger for this thread with data_type
    logger = setup_logging("Main", data_type=data_type)
    
    if not config['kpi_formulas'] and not config['kpi_families']:
        logger.warning(f"Skipping {data_type} processing: Empty KPI formulas and families")
        return

    logger.info(f"Starting processing for {data_type} data")
    try:
        # Initialize the transformer with configurations
        transformer = Transformer(
            source_db_config=config['source_db_config'],
            dest_db_config=config['dest_db_config'],
            kpi_formulas=config['kpi_formulas'],
            kpi_families=config['kpi_families'],
            node_pattern=config['node_pattern'],
            suffix_operator_mapping=config['suffix_operator_mapping'],
            file_path=config['file_path'],
            data_type=data_type  # Pass data_type to Transformer
        )
        
        # Run the transformation process
        transformer.process()
        logger.info(f"Completed processing for {data_type} data")
        
    except Exception as e:
        logger.error(f"Error processing {data_type} data: {e}")
        raise

if __name__ == "__main__":
    # Parallel processing with threading
    threads = []
    for data_type, config in CONFIGS.items():
        t = threading.Thread(target=run_transformer, args=(config, data_type))
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Initialize a main logger for final message (not thread-specific)
    logger = setup_logging("Main")
    logger.info("All threads completed")