import logging

import numpy as np

from .database import Database


class GenericTable(Database):
    """Interact with the database. See README for further details"""

    __slots__ = ["name", "id_col"]

    def __init__(self, clear=False, **kwargs):
        """Validates name subclass attr. Creates data dir. Inits tables"""

        assert hasattr(self, "name"), "Subclass MUST have a table name attr"
        id_col_err = "Subclass must have an id_col attr, even if it's None"
        assert hasattr(self, "id_col"), id_col_err

        # Connect
        super(GenericTable, self).__init__(**kwargs)

        # Clears table
        if clear:
            self.clear_table()

        # Creates table
        self.create_table()

    def clear_table(self):
        """Clears the table"""

        logging.debug(f"Dropping {self.name} Table")
        self.execute(f"DROP TABLE IF EXISTS {self.name} CASCADE")
        logging.debug(f"{self.name} Table dropped")

    def insert(self, data: dict):
        """Inserts a dictionary into the database, and returns id_col"""

        assert isinstance(data, dict)

        #  NOTE: you only need to convert lists for CSVs!!! Not here...
        for key, val in data.items():
            # Can't have this in one place because numpy.ndarray is the type
            # But numpy.array is the comprehension
            if isinstance(val, tuple):
                data[key] = list(data[key])
            elif isinstance(val, np.ndarray):
                # Must convert inner types to not be numpy types
                data[key] = list(float(x) for x in data[key])
        values_str = ", ".join(["%s"] * len(data))

        sql = f"""INSERT INTO {self.name} ({",".join(data.keys())})
                    values ({values_str})"""

        if self.id_col:
            sql += f" RETURNING {self.id_col};"

        logging.debug(sql)

        result = self.execute(sql, tuple(data.values()))

        # Return the new ID
        if self.id_col:
            return result[0][self.id_col]

    def get_all(self) -> list:
        """Gets all rows from table"""

        return self.execute(f"SELECT * FROM {self.name}")

    def get_count(self, sql: str = None, data: list = []) -> int:
        """Gets count from table"""

        if data:
            assert sql

        sql = sql if sql else f"SELECT COUNT(*) FROM {self.name}"
        input(self.execute(sql, data))
        return self.execute(sql, data)[0]["count"]

    def copy_to_tsv(self, path: str):
        """Copies table to a specified path"""

        logging.debug(f"Copying file from {self.name} to {path}")
        # NOTE: can't use the easy method due to AWS
        # self.execute(f"COPY {self.name} TO %s DELIMITER '\t';", [path])
        
        run_cmds(
        logging.debug("Copy complete")
