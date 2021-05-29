#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module contains class Generic Table
The Generic Table class can interact with a database. It can also be
inherited to allow for its functions to be used for specific tables in
the database. Other Table classes inherit the database class to be used
in utils functions that write data to the database. To do this, the
class that inherits the database must be named the table name plus _Table.
Note that all tables should have:
_create_tables - this creates empty tables. 
Sometimes unessecary, don't always need
create_index - creates an index on the table
fill_table - fills table with data, sometimes unessecary
clear_table - inherited, clears table
There are also some convenience funcs, documented below
"""

__authors__ = ["Justin Furuness"]
__credits__ = ["Justin Furuness"]
__Lisence__ = "BSD"
__maintainer__ = "Justin Furuness"
__email__ = "jfuruness@gmail.com"
__status__ = "Development"

import inspect
import warnings
import logging
from multiprocessing import cpu_count
from subprocess import check_call
import os
import time

import csv
import numpy as np
from psycopg2.extensions import AsIs

from .database import Database


class GenericTable(Database):
    """Interact with the database"""

    __slots__ = ["name"]

    data_dir = "/data/"

    def __init__(self, *args, **kwargs):
        """Asserts that name is set
        Makes sure sql queries are formed properly"""

        assert hasattr(self, "name"), "Inherited class MUST have a table name attr"
        unlogged_err = ("Create unlogged tables for speed.\n Ex:"
                        "CREATE UNLOGGED TABLE IF NOT EXISTS {self.name}...")
        # https://stackoverflow.com/a/427533/8903959
        if "create table" in inspect.getsource(self.__class__):
            raise Exception(unlogged_err + "\n And also capitalize SQL")
        if "CREATE TABLE" in inspect.getsource(self.__class__):
            raise Exception(unlogged_err)
        if not os.path.exists(self.data_dir):
            raise Exception("/data/ does not exist. Make and chown")

        super(GenericTable, self).__init__(*args, **kwargs)

    def insert(self, data: dict):

        assert isinstance(data, dict)

        for key, val in data.items():
            if isinstance(val, list) or isinstance(val, np.ndarray):
                data[key] = "{" + ", ".join([str(x) for x in val]) + "}"

        values_str = ", ".join(["%s"] * len(data))
        sql = f"""INSERT INTO {self.name} ({",".join(data.keys())})
                    values ({values_str})"""
        if hasattr(self, "id_col"):
            sql += f" RETURNING {self.id_col};"
        logging.debug(sql)
        result = self.execute(sql, tuple(data.values()))
        if result:
            return result[0][self.id_col]

    def tsv_insert(self, data: dict):
        assert isinstance(data, dict)

        # List formatting for postgres
        for key, val in data.items():
            if isinstance(val, list) or isinstance(val, np.ndarray):
                data[key] = "{" + ", ".join([str(x) for x in val]) + "}"

        with open(os.path.join(self.data_dir, self.name, newline=''), "a+") as f:
            writer = csv.DictWriter(f, fieldnames=data.keys(), delimiter="\t")
            writer.writerow(data)

    def get_all(self) -> list:
        """Gets all rows from table"""

        return self.execute(f"SELECT * FROM {self.name}")

    def get_count(self, sql: str = None, data: list = []) -> int:
        """Gets count from table"""

        sql = sql if sql else f"SELECT COUNT(*) FROM {self.name}"
        return self.execute(sql, data)[0]["count"]


    def clear_table(self):
        """Clears the table"""

        logging.debug(f"Dropping {self.name} Table")
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.name} CASCADE")
        logging.debug(f"{self.name} Table dropped")

    def copy_table(self, path: str):
        """Copies table to a specified path"""

        logging.debug(f"Copying file from {self.name} to {path}")
        self.execute(f"COPY {self.name} TO %s DELIMITER '\t';", [path])
        logging.debug("Copy complete")

    @property
    def columns(self) -> list:
        """Returns the columns of the table
        used in utils to insert csv into the database"""

        sql = """SELECT column_name FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = %s;
              """
        self.cursor.execute(sql, [self.name])
        # Make sure that we don't get the _id columns
        return [x['column_name'] for x in self.cursor.fetchall()
                if self.col_id not in x['column_name']]
