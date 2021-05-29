import logging
import psycopg2
from psycopg2.extras import RealDictCursor

from lib_config import Config
from lib_utils.helper_funcs import retry


class Database:
    """Interact with the database. See README for further details"""

    default_database = "main"

    def __init__(self, cursor_factory=RealDictCursor):
        """Create a new connection with the database"""

        self._connect(cursor_factory)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @retry(psycopg2.OperationalError, msg="DB connection failure", sleep=10)
    def _connect(self, cursor_factory=RealDictCursor):
        """Connects to db with default RealDictCursor.
        Note that RealDictCursor returns everything as a dictionary."""

        # Database needs access to the section header
        with Config(write=False) as conf_dict:
            conf_dict[self.default_database]["cursor_factory"] = cursor_factory

        # In case the database is somehow off we wait
        _conn = psycopg2.connect(**conf_dict)

        logging.debug("Database Connected")
        self._conn = _conn
        # Automatically execute queries
        self._conn.autocommit = True
        self._cursor = _conn.cursor()

    def execute(self, sql: str, data: iter = []) -> list:
        """Executes a query. Returns [] if no results."""

        assert (isinstance(data, list)
                or isinstance(data, tuple)), "Data must be list/tuple"
        self.cursor.execute(sql, data)

        try:
            return self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            return []

    def close(self):
        """Closes the database connection correctly"""

        self._cursor.close()
        self._conn.close()
