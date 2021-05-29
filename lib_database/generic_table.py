import logging

import numpy as np

from .database import Database


class ImproperDevelopment(Exception):
    pass


class GenericTable(Database):
    """Interact with the database. See README for further details"""

    __slots__ = ["name", "id_col"]

    def __init__(self, clear=False, **kwargs):
        """Validates name subclass attr. Creates data dir. Inits tables"""

        assert hasattr(self, "name"), "Subclass MUST have a table name attr"
        id_col_err = "Subclass must have an id_col attr, even if it's None"
        assert hasattr(self, "id_col"), id_col_err

        # Clears table
        if clear:
            self.clear_table()

        # Creates table
        self.create_table()

        super(GenericTable, self).__init__(**kwargs)

    def clear_table(self):
        """Clears the table"""

        logging.debug(f"Dropping {self.name} Table")
        self.execute(f"DROP TABLE IF EXISTS {self.name} CASCADE")
        logging.debug(f"{self.name} Table dropped")

    def create_table(self):
        """A function to be inherited that creates the table"""

        pass

    def insert(self, data: dict):
        """Inserts a dictionary into the database, and returns id_col"""

        assert isinstance(data, dict)

        for key, val in data.items():
            if (isinstance(val, list)
                    or isinstance(val, np.ndarray)
                    or isinstance(val, tuple)):

                data[key] = "{" + ", ".join([str(x) for x in val]) + "}"

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

        sql = sql if sql else f"SELECT COUNT(*) FROM {self.name}"
        return self.execute(sql, data)[0]["count"]

    def copy_table_to_tsv(self, path: str):
        """Copies table to a specified path"""

        logging.debug(f"Copying file from {self.name} to {path}")
        self.execute(f"COPY {self.name} TO %s DELIMITER '\t';", [path])
        logging.debug("Copy complete")

    @property
    def columns(self) -> list:
        """Returns the columns of the table

        used in utils to insert csv into the database"""

        sql = """SELECT column_name FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = %s;"""

        return [x['column_name'] for x in self.execute(sql, [self.name])]
