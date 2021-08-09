import logging
import psycopg2
from psycopg2.extras import RealDictCursor

from lib_utils.helper_funcs import retry

from .postgres import Postgres, DEFAULT_CONF_SECTION


class Database(Postgres):
    """Interact with the database. See README for further details"""

    def __init__(self,
                 conf_section=DEFAULT_CONF_SECTION,
                 cursor_factory=RealDictCursor):
        """Create a new connection with the database"""

        self._connect(conf_section, cursor_factory)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @retry(psycopg2.OperationalError, msg="DB connection failure")
    def _connect(self, config_section, cursor_factory):
        """Connects to db with default RealDictCursor.
        Note that RealDictCursor returns everything as a dictionary."""

        # In case the database is somehow off we wait
        _conn = psycopg2.connect(cursor_factory=cursor_factory,
                                 **self._get_db_creds(config_section))

        logging.debug("Database Connected")
        self._conn = _conn
        # Automatically execute queries
        self._conn.autocommit = True
        self._cursor = _conn.cursor()

    def execute(self, sql: str, data: iter = []) -> list:
        """Executes a query. Returns [] if no results."""

        assert (isinstance(data, list)
                or isinstance(data, tuple)), "Data must be list/tuple"
        self._cursor.execute(sql, data)

        try:
            return self._cursor.fetchall()
        except psycopg2.ProgrammingError:
            return []

    def close(self):
        """Closes the database connection correctly"""

        self._cursor.close()
        self._conn.close()
