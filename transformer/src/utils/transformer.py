import pandas as pd
from typing import Dict, List, Any
from utils.logger import setup_logging
from utils.tools import (
    connect_database,
    create_tables,
    extract_noeud,
    extract_indicateur_suffixe,
)


class Transformer:

    def __init__(
        self,
        source_db_config: Dict[str, Any],
        dest_db_config: Dict[str, Any],
        kpi_formulas: Dict[str, Any],
        kpi_families: Dict[str, Any],
        node_pattern: str,
        suffix_operator_mapping: Dict[str, Any],
        file_path: str,
        data_type: str,
    ) -> None:
        """Initialize the Transformer with database configurations."""
        self.source_conn = connect_database(source_db_config, data_type =data_type)
        self.source_cursor = self.source_conn.cursor()
        self.dest_conn = connect_database(dest_db_config, data_type =data_type)
        self.dest_cursor = self.dest_conn.cursor()
        self.kpi_formulas = kpi_formulas
        self.kpi_families = kpi_families
        self.noeud_pattern = node_pattern
        self.logger = setup_logging("Transformer", data_type=data_type)
        self.suffix_operator_mapping = suffix_operator_mapping
        self.file_path = file_path
        self.tables = self.load_tables()
        self.data_type = data_type

    def load_tables(self) -> List[str]:
        """Load table names from result_type.txt."""
        try:
            with open(self.file_path, "r") as f:
                tables = [line.strip() for line in f if line.strip()]
            self.logger.info(
                f"Loaded {len(tables)} tables from {self.file_path}: {tables}"
            )
            return tables
        except Exception as e:
            self.logger.error(f"Error loading tables from file: {e}")
            raise

    def create_tables(self):
        """Create tables in the destination database."""
        try:
            create_tables(self.dest_cursor, self.kpi_formulas, self.kpi_families, self.data_type)
            self.dest_conn.commit()
            self.logger.info("Tables created successfully in destination database.")
        except Exception as e:
            self.logger.error(f"Error creating tables in destination database: {e}")
            self.dest_conn.rollback()
            raise

    def get_distinct_dates(self, table: str) -> List[str]:
        """Retrieve distinct Date values from a table in the source database."""
        try:
            query = f"SELECT DISTINCT Date FROM {table}"
            self.source_cursor.execute(query)
            dates = [str(row[0]) for row in self.source_cursor.fetchall()]
            self.logger.info(f"Extracted {len(dates)} distinct dates from {table}")
            return dates
        except Exception as e:
            self.logger.error(f"Error getting distinct dates from {table}: {e}")
            raise

    def extract_node(self, table: str) -> str:
        """Extract Node from table name."""
        matches = extract_noeud(self.noeud_pattern, [table], self.data_type)
        if matches:
            node = matches[0][1]
            self.logger.info(f"Extracted node '{node}' from table '{table}'")
            return node
        self.logger.warning(f"No node found in table name: {table}")
        return None

    def filter_indicateur_values(
        self, table: str, date: str, kpi: str = None, family: str = None
    ) -> pd.DataFrame:
        """Filter indicateur values for a specific KPI or family and date from the source database."""
        if family:
            kpis = self.kpi_families[family]
            prefixes = set()
            for k in kpis:
                config = self.kpi_formulas[k]
                prefixes.update(
                    config.get("numerator", [])
                    + config.get("denominator", [])
                    + config.get("additional", [])
                )
            prefixes = list(prefixes)
        else:
            kpi_config = self.kpi_formulas[kpi]
            prefixes = (
                kpi_config.get("numerator", [])
                + kpi_config.get("denominator", [])
                + kpi_config.get("additional", [])
            )

        try:
            query = f"""
                SELECT indicateur, valeur
                FROM {table}
                WHERE Date = %s AND ({' OR '.join(['indicateur LIKE %s' for _ in prefixes])})
            """
            params = [date] + [f"{prefix}%" for prefix in prefixes]
            self.source_cursor.execute(query, params)
            data = self.source_cursor.fetchall()

            df = pd.DataFrame(data, columns=["indicateur", "valeur"])
            if df.empty:
                self.logger.warning(
                    f"No data found for {kpi or family} on {date} in {table}"
                )
            else:
                self.logger.info(
                    f"Filtered {len(df)} indicateur values for {kpi or family} on {date} from {table}"
                )
            return df
        except Exception as e:
            self.logger.error(
                f"Error filtering indicateur values for {kpi or family} from {table}: {e}"
            )
            raise

    def group_by_suffix(self, df: pd.DataFrame, family: str) -> List[Dict[str, Any]]:
        """Group filtered data by full suffix for family KPIs, collecting all KPI values."""
        grouped = {}
        for _, row in df.iterrows():
            prefix, suffix = extract_indicateur_suffixe(row["indicateur"], self.data_type)
            if not suffix:
                self.logger.warning(f"No suffix found for indicateur: {row['indicateur']}")
                continue

            # Validate suffix
            if not suffix or suffix == "M":
                self.logger.warning(
                    f"Skipping invalid suffix for family {family}: {suffix}"
                )
                continue

            if suffix not in grouped:
                grouped[suffix] = {"kpi_values": {}, "group_values": {}}

            # Determine which KPI this counter belongs to
            for kpi in self.kpi_families[family]:
                kpi_config = self.kpi_formulas[kpi]
                if kpi not in grouped[suffix]["group_values"]:
                    grouped[suffix]["group_values"][kpi] = {
                        "numerator": [],
                        "denominator": [],
                        "additional": [],
                    }

                if prefix in kpi_config.get("numerator", []):
                    grouped[suffix]["group_values"][kpi]["numerator"].append(
                        float(row["valeur"])
                    )
                elif prefix in kpi_config.get("denominator", []):
                    grouped[suffix]["group_values"][kpi]["denominator"].append(
                        float(row["valeur"])
                    )
                elif prefix in kpi_config.get("additional", []):
                    grouped[suffix]["group_values"][kpi]["additional"].append(
                        float(row["valeur"])
                    )

        # Calculate KPI values
        result = []
        for suffix, data in grouped.items():
            for kpi in self.kpi_families[family]:
                if kpi in data["group_values"]:
                    kpi_value = self.calculate_kpi(kpi, data["group_values"][kpi])
                    data["kpi_values"][kpi] = kpi_value
                else:
                    data["kpi_values"][kpi] = None
            result.append(
                {
                    "suffix": suffix,
                    "kpi_values": data["kpi_values"],
                    "group_values": data["group_values"],
                }
            )

        self.logger.info(
            f"Grouped data by suffix for family {family}: {[item['suffix'] for item in result]}"
        )
        return result

    def calculate_group_values(
        self, df: pd.DataFrame, kpi_config: Dict
    ) -> Dict[str, List[float]]:
        """Calculate values for numerator, denominator, and additional fields."""
        result = {"numerator": [], "denominator": [], "additional": []}
        for _, row in df.iterrows():
            prefix, _ = extract_indicateur_suffixe(row["indicateur"], self.data_type)
            if prefix in kpi_config.get("numerator", []):
                result["numerator"].append(float(row["valeur"]))
            elif prefix in kpi_config.get("denominator", []):
                result["denominator"].append(float(row["valeur"]))
            elif prefix in kpi_config.get("additional", []):
                result["additional"].append(float(row["valeur"]))
        return result

    def calculate_kpi(self, kpi: str, group_values: Dict[str, List[float]]) -> float:
        """Calculate KPI value using the formula."""
        kpi_config = self.kpi_formulas[kpi]
        formula = kpi_config["formula"]

        try:
            if "additional" in kpi_config:
                result = formula(
                    group_values["numerator"],
                    group_values["denominator"],
                    group_values["additional"],
                )
            elif "denominator" in kpi_config:
                result = formula(group_values["numerator"], group_values["denominator"])
            else:
                result = formula(group_values["numerator"])
            self.logger.info(
                f"Calculated {kpi} value: {result}, numerator={group_values['numerator']}, denominator={group_values.get('denominator', [])}, additional={group_values.get('additional', [])}"
            )
            return result
        except ZeroDivisionError:
            self.logger.warning(
                f"ZeroDivisionError calculating {kpi}: denominator={group_values.get('denominator', [])}"
            )
            return None
        except Exception as e:
            self.logger.error(f"Error calculating {kpi}: {e}")
            return None

    def insert_kpi_summary(self, date: str, node: str) -> int:
        """Insert into kpi_summary in the destination database and return the generated ID."""
        try:
            query = "INSERT INTO kpi_summary (Date, Node) VALUES (%s, %s)"
            self.dest_cursor.execute(query, (date, node))
            self.dest_conn.commit()
            self.dest_cursor.execute("SELECT LAST_INSERT_ID()")
            kpi_id = self.dest_cursor.fetchone()[0]
            self.logger.info(
                f"Inserted into kpi_summary: Date={date}, Node={node}, ID={kpi_id}"
            )
            return kpi_id
        except Exception as e:
            self.logger.error(f"Error inserting into kpi_summary: {e}")
            self.dest_conn.rollback()
            raise

    def insert_kpi_details(
        self,
        kpi: str,
        kpi_id: int,
        suffix: str,
        group_values: Dict[str, List[float]],
        kpi_value: float,
    ):
        """Insert into non-family KPI details table."""
        kpi_config = self.kpi_formulas[kpi]
        table_name = f"{kpi}_details"

        column_value_map = {"kpi_id": kpi_id}
        if kpi_config.get("Suffix", False) and suffix:
            column_value_map["suffix"] = suffix
            normalized_suffix = suffix.lower()
            operator = "Unknown"
            if "nw" in normalized_suffix and (
                "ie" in normalized_suffix or "is" in normalized_suffix
            ):
                operator = "Inwi International"
            else:
                for op_suffix in self.suffix_operator_mapping.keys():
                    if op_suffix in normalized_suffix:
                        operator = self.suffix_operator_mapping[op_suffix]
                        break
            column_value_map["operator"] = operator
            if operator == "Unknown":
                self.logger.warning(
                    f"No known operator found in suffix: {suffix} (normalized: {normalized_suffix})"
                )
        else:
            column_value_map["operator"] = None

        for field in ["numerator", "denominator", "additional"]:
            if field in kpi_config:
                for i, prefix in enumerate(kpi_config[field]):
                    value = (
                        sum(group_values[field][i : i + 1])
                        if i < len(group_values[field])
                        else 0
                    )
                    column_value_map[prefix] = value

        column_value_map["value"] = kpi_value

        columns = list(column_value_map.keys())
        values = list(column_value_map.values())
        params = ["%s"] * len(columns)

        try:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(params)})"
            self.dest_cursor.execute(query, values)
            self.dest_conn.commit()
            operator_log = column_value_map.get("operator", "None")
            self.logger.info(
                f"Inserted into {table_name}: kpi_id={kpi_id}, suffix={suffix}, operator={operator_log}, columns={columns}"
            )
        except Exception as e:
            self.logger.error(f"Error inserting into {table_name}: {e}")
            self.dest_conn.rollback()
            raise

    def insert_family_details(
        self,
        family: str,
        kpi_id: int,
        suffix: str,
        kpi_values: Dict[str, float],
        group_values: Dict[str, Dict[str, List[float]]],
    ):
        """Insert into family details table with KPI-specific columns and family_sum."""
        table_name = f"{family}_details"

        column_value_map = {
            "kpi_id": kpi_id,
            "suffix": suffix
        }

        # Determine operator from suffix
        normalized_suffix = suffix.lower()
        operator = "Unknown"
        if "nw" in normalized_suffix and (
            "ie" in normalized_suffix or "is" in normalized_suffix
        ):
            operator = "Inwi International"
        else:
            for op_suffix in self.suffix_operator_mapping.keys():
                if op_suffix in normalized_suffix:
                    operator = self.suffix_operator_mapping[op_suffix]
                    break
        column_value_map["operator"] = operator
        if operator == "Unknown":
            self.logger.warning(
                f"No known operator found in suffix: {suffix} (normalized: {normalized_suffix})"
            )

        # Aggregate counter values for numerators, denominators, and additionals
        all_counters = set()
        numerator_fields = set()
        denominator_fields = set()
        additional_fields = set()
        for kpi in self.kpi_families[family]:
            kpi_config = self.kpi_formulas[kpi]
            numerator_fields.update(kpi_config.get("numerator", []))
            denominator_fields.update(kpi_config.get("denominator", []))
            additional_fields.update(kpi_config.get("additional", []))
            all_counters.update(
                kpi_config.get("numerator", [])
                + kpi_config.get("denominator", [])
                + kpi_config.get("additional", [])
            )

        for counter in all_counters:
            counter_value = 0
            for kpi, values in group_values.items():
                kpi_config = self.kpi_formulas[kpi]
                for field in ["numerator", "denominator", "additional"]:
                    if field in kpi_config and counter in kpi_config[field]:
                        idx = kpi_config[field].index(counter)
                        counter_value += (
                            sum(values[field][idx : idx + 1])
                            if idx < len(values[field])
                            else 0
                        )
            column_value_map[counter] = counter_value

        # Add KPI-specific values (kpis_calculated)
        for kpi, value in kpi_values.items():
            column_value_map[kpi] = value

        # Calculate family_sum
        valid_values = [v for v in kpi_values.values() if v is not None]
        family_sum = sum(valid_values) if valid_values else None
        column_value_map["family_sum"] = family_sum

        columns = list(column_value_map.keys())
        values = list(column_value_map.values())
        params = ["%s"] * len(columns)

        try:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(params)})"
            self.dest_cursor.execute(query, values)
            self.dest_conn.commit()
            self.logger.info(
                f"Inserted into {table_name}: kpi_id={kpi_id}, suffix={suffix}, operator={operator}, kpi_values={kpi_values}, family_sum={family_sum}"
            )
        except Exception as e:
            self.logger.error(f"Error inserting into {table_name}: {e}")
            self.dest_conn.rollback()
            raise

    def process(self):
        """Main process to handle all tables."""
        self.create_tables()

        for table in self.tables:
            node = self.extract_node(table)
            if not node:
                continue

            dates = self.get_distinct_dates(table)
            for date in dates:
                kpi_summary_id = self.insert_kpi_summary(date, node)

                # Process family-based KPIs
                for family, kpis in self.kpi_families.items():
                    df = self.filter_indicateur_values(table, date, family=family)
                    grouped_data = self.group_by_suffix(df, family)

                    for group in grouped_data:
                        suffix = group["suffix"]
                        kpi_values = group["kpi_values"]
                        group_values = group["group_values"]
                        self.insert_family_details(
                            family, kpi_summary_id, suffix, kpi_values, group_values
                        )

                # Process non-family KPIs
                for kpi in self.kpi_formulas.keys():
                    if self.kpi_formulas[kpi].get("family") in self.kpi_families:
                        continue
                    df = self.filter_indicateur_values(table, date, kpi=kpi)
                    grouped_data = self.group_by_suffix(df, kpi)

                    for group in grouped_data:
                        suffix = group["suffix"]
                        group_values = group["values"]
                        kpi_value = self.calculate_kpi(kpi, group_values)
                        self.insert_kpi_details(
                            kpi, kpi_summary_id, suffix, group_values, kpi_value
                        )

    def group_by_suffix(
        self, df: pd.DataFrame, kpi_or_family: str
    ) -> List[Dict[str, Any]]:
        """Group filtered data by full suffix, handling both KPIs and families."""
        if kpi_or_family in self.kpi_families:
            return self.group_by_suffix_for_family(df, kpi_or_family)
        else:
            kpi_config = self.kpi_formulas[kpi_or_family]
            if not kpi_config.get("Suffix", False):
                return [
                    {
                        "suffix": "",
                        "values": self.calculate_group_values(df, kpi_config),
                    }
                ]

            grouped = {}
            for _, row in df.iterrows():
                prefix, suffix = extract_indicateur_suffixe(row["indicateur"], self.data_type)
                if not suffix:
                    self.logger.warning(
                        f"No suffix found for indicateur: {row['indicateur']}"
                    )
                    continue

                if not suffix or suffix == "M":
                    self.logger.warning(
                        f"Skipping invalid suffix for {kpi_or_family}: {suffix}"
                    )
                    continue

                if suffix not in grouped:
                    grouped[suffix] = {
                        "numerator": [],
                        "denominator": [],
                        "additional": [],
                    }

                if prefix in kpi_config.get("numerator", []):
                    grouped[suffix]["numerator"].append(float(row["valeur"]))
                elif prefix in kpi_config.get("denominator", []):
                    grouped[suffix]["denominator"].append(float(row["valeur"]))
                elif prefix in kpi_config.get("additional", []):
                    grouped[suffix]["additional"].append(float(row["valeur"]))

            result = [
                {
                    "suffix": suffix,
                    "values": {
                        "numerator": data["numerator"],
                        "denominator": data["denominator"],
                        "additional": data["additional"],
                    },
                }
                for suffix, data in grouped.items()
            ]

            self.logger.info(
                f"Grouped data by suffix for {kpi_or_family}: {[item['suffix'] for item in result]}"
            )
            return result

    def group_by_suffix_for_family(
        self, df: pd.DataFrame, family: str
    ) -> List[Dict[str, Any]]:
        """Group filtered data by full suffix for family KPIs, collecting all KPI values."""
        grouped = {}
        for _, row in df.iterrows():
            prefix, suffix = extract_indicateur_suffixe(row["indicateur"], self.data_type)
            if not suffix:
                self.logger.warning(f"No suffix found for indicateur: {row['indicateur']}")
                continue

            if not suffix or suffix == "M":
                self.logger.warning(
                    f"Skipping invalid suffix for family {family}: {suffix}"
                )
                continue

            if suffix not in grouped:
                grouped[suffix] = {"kpi_values": {}, "group_values": {}}

            for kpi in self.kpi_families[family]:
                kpi_config = self.kpi_formulas[kpi]
                if kpi not in grouped[suffix]["group_values"]:
                    grouped[suffix]["group_values"][kpi] = {
                        "numerator": [],
                        "denominator": [],
                        "additional": [],
                    }

                if prefix in kpi_config.get("numerator", []):
                    grouped[suffix]["group_values"][kpi]["numerator"].append(
                        float(row["valeur"])
                    )
                elif prefix in kpi_config.get("denominator", []):
                    grouped[suffix]["group_values"][kpi]["denominator"].append(
                        float(row["valeur"])
                    )
                elif prefix in kpi_config.get("additional", []):
                    grouped[suffix]["group_values"][kpi]["additional"].append(
                        float(row["valeur"])
                    )

        result = []
        for suffix, data in grouped.items():
            for kpi in self.kpi_families[family]:
                if kpi in data["group_values"]:
                    kpi_value = self.calculate_kpi(kpi, data["group_values"][kpi])
                    data["kpi_values"][kpi] = kpi_value
                else:
                    data["kpi_values"][kpi] = None
            result.append(
                {
                    "suffix": suffix,
                    "kpi_values": data["kpi_values"],
                    "group_values": data["group_values"],
                }
            )

        self.logger.info(
            f"Grouped data by suffix for family {family}: {[item['suffix'] for item in result]}"
        )
        return result

    def __del__(self):
        """Cleanup database connections."""
        self.source_cursor.close()
        self.source_conn.close()
        self.dest_cursor.close()
        self.dest_conn.close()
        self.logger.info("Database connections closed.")



