import MySQLdb
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.logger import setup_logging

# Initialize logger (data_type will be passed dynamically or set to None if called directly)
def get_tools_logger(data_type=None):
    return setup_logging("Tools", data_type=data_type)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_database(config: Dict[str, Any], data_type=None):
    """Connect to the database using mysqlclient with retries."""
    logger = get_tools_logger(data_type)
    try:
        logger.info(f"Connecting to database config: {config}")
        conn = MySQLdb.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['password'],
            port=config['port'],
            db=config['database']
        )
        logger.info(f"Successfully connected to database: {config['database']} on {config['host']}")
        return conn
    except MySQLdb.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def create_main_table(cursor, data_type=None):
    """Create the main table if it doesn't exist."""
    logger = get_tools_logger(data_type)
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kpi_summary (
                Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                Date DATETIME NOT NULL,
                Node VARCHAR(50) NOT NULL
            );
        """)
        logger.info("✅ Main table 'kpi_summary' created or already exists.")
    except MySQLdb.Error as e:
        logger.error(f"Error creating main table: {e}")
        raise

def create_kpi_tables(cursor, KPI_FORMULAS, KPI_FAMILIES, data_type=None):
    """Create KPI-specific tables based on KPI_FORMULAS and KPI_FAMILIES config."""
    logger = get_tools_logger(data_type)
    try:
        # Create family-based tables
        for family, kpis in KPI_FAMILIES.items():
            columns_set = {
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
                "kpi_id INT NOT NULL",
                "operator VARCHAR(50)",
                "suffix VARCHAR(50)",
                "type VARCHAR(10)"
            }
            numerator_fields = set()
            denominator_fields = set()
            additional_fields = set()
            for kpi in kpis:
                config = KPI_FORMULAS[kpi]
                num_fields = config.get('numerator', [])
                denom_fields = config.get('denominator', [])
                add_fields = config.get('additional', [])
                numerator_fields.update(num_fields)
                denominator_fields.update(denom_fields)
                additional_fields.update(add_fields)
            
            ordered_columns = [
                "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
                "kpi_id INT NOT NULL",
                "operator VARCHAR(50)",
                "suffix VARCHAR(50)",
            ]
            ordered_columns.extend(sorted([f"{col} FLOAT" for col in numerator_fields]))
            ordered_columns.extend(sorted([f"{col} FLOAT" for col in denominator_fields if col not in numerator_fields]))
            ordered_columns.extend(sorted([f"{col} FLOAT" for col in additional_fields if col not in numerator_fields and col not in denominator_fields]))
            ordered_columns.extend(sorted([f"{kpi} FLOAT" for kpi in kpis]))
            ordered_columns.append("family_sum FLOAT")
            
            columns_str = ",\n    ".join(ordered_columns)
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {family}_details (
                {columns_str},
                FOREIGN KEY (kpi_id) REFERENCES kpi_summary(Id) 
            );
            """
            cursor.execute(create_query)
            logger.info(f"✅ Table '{family}_details' created or already exists.")

        # Create individual tables for KPIs not in a family
        for kpi, config in KPI_FORMULAS.items():
            if config.get('family') in KPI_FAMILIES:
                continue
            columns_set = {"kpi_id INT NOT NULL"}
            if config.get('Suffix', False):
                columns_set.add("suffix VARCHAR(50)")
            columns_set.add("operator VARCHAR(50)")
            all_fields = (
                config.get('numerator', []) +
                config.get('denominator', []) +
                config.get('additional', [])
            )
            for col in all_fields:
                columns_set.add(f"{col} FLOAT")
            columns_set.add("value FLOAT")
            columns_str = ",\n    ".join(sorted(columns_set))
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {kpi}_details (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {columns_str},
                FOREIGN KEY (kpi_id) REFERENCES kpi_summary(Id)
            );
            """
            cursor.execute(create_query)
            logger.info(f"✅ Table '{kpi}_details' created or already exists.")
    except MySQLdb.Error as e:
        logger.error(f"Error creating KPI tables: {e}")
        raise

def create_tables(cursor, KPI_FORMULAS, KPI_FAMILIES, data_type=None):
    """Create all necessary tables in the database."""
    logger = get_tools_logger(data_type)
    try:
        create_main_table(cursor, data_type)
        create_kpi_tables(cursor, KPI_FORMULAS, KPI_FAMILIES, data_type)
        logger.info("✅ All tables created successfully.")
    except MySQLdb.Error as e:
        logger.error(f"Error creating tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating tables: {e}")
        raise

def extract_noeud(pattern, texts, data_type=None):
    """Extracts prefixes from the provided list of texts using the given regex pattern."""
    logger = get_tools_logger(data_type)
    matches = []
    for text in texts:
        match = pattern.match(text)
        if match:
            prefix = match.group(1).upper()
            matches.append((text, prefix))
            logger.debug(f"Extracted node prefix '{prefix}' from text '{text}'")
    return matches

def extract_indicateur_suffixe(indicateur, data_type=None):
    """Extract the suffix from the KPI name."""
    logger = get_tools_logger(data_type)
    if not isinstance(indicateur, str):
        logger.error("Indicateur must be a string")
        raise ValueError("Indicateur must be a string")
    
    parts = indicateur.split('.')
    if len(parts) == 2:
        logger.debug(f"Extracted indicateur '{parts[0]}' with suffix '{parts[1]}'")
        return parts[0], parts[1]
    
    logger.debug(f"Extracted indicateur '{parts[0]}' with no suffix")
    return parts[0], None