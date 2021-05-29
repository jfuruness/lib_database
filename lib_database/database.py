#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module contains class Database. See README for in depth details

originally from lib_bgp_data"""

__authors__ = ["Justin Furuness"]
__credits__ = ["Justin Furuness"]
__maintainer__ = "Justin Furuness"
__email__ = "jfuruness@gmail.com"


import logging
import os
import time

import psycopg2
from psycopg2.extras import RealDictCursor

from .postgres import Postgres

from lib_utils.utils import config_logging
from lib_utils import utils


class Database(Postgres):
    """Interact with the database"""

    __slots__ = ['conn', 'cursor', '_clear']

    def __init__(self, cursor_factory=RealDictCursor, clear=False):
        """Create a new connection with the database"""

        # Initializes self.logger
        config_logging()
        self._connect(cursor_factory)
        self._clear = clear

    def __enter__(self):
        """This allows this class to be used as a context manager
        With this you don't need to worry about closing connections.
        """

        # Checks if it has attributes, because this parent class does not
        # Only the generic table has these attributes
        # If clear is set clear the table
        if self._clear and hasattr(self, "clear_table"):
            self.clear_table()
        return self

    def __exit__(self, type, value, traceback):
        """Closes connection, exits contextmanager"""

        self.close()
        
    def _connect(self, cursor_factory=RealDictCursor):
        """Connects to db with default RealDictCursor.
        Note that RealDictCursor returns everything as a dictionary."""
        # Database needs access to the section header
        kwargs = Config().get_db_creds()
        if cursor_factory:
            kwargs["cursor_factory"] = cursor_factory
        # In case the database is somehow off we wait
        for i in range(10):
            try:
                conn = psycopg2.connect(**kwargs)

                logging.debug("Database Connected")
                self.conn = conn
                # Automatically execute queries
                self.conn.autocommit = True
                self.cursor = conn.cursor()
                break
            except psycopg2.OperationalError as e:
                logging.warning(f"Couldn't connect to db {e}")
                time.sleep(10)
        if hasattr(self, "_create_tables"):
            # Creates tables if do not exist
            self._create_tables()

    def execute(self, sql: str, data: iter = []) -> list:
        """Executes a query. Returns [] if no results."""

        assert (isinstance(data, list)
                or isinstance(data, tuple)), "Data must be list/tuple"
        self.cursor.execute(sql, data)

        try:
            return self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            return []

    def close(self):
        """Closes the database connection correctly"""

        self.cursor.close()
        self.conn.close()
