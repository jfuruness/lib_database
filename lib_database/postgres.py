#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module contains class postgres for modifying postgres config

originally from lib_bgp_data
"""

__author__ = "Justin Furuness"
__credits__ = ["Justin Furuness"]
__maintainer__ = "Justin Furuness"
__email__ = "jfuruness@gmail.com"


import logging
import os
import random
import string
import time


from .config import Config
from lib_utils import utils



# NOTE: This class is mostly obsolete if we are using AWS
# If we ever move to our own server, many of these functions will be useful


SQL_FILE_PATH = "/tmp/db_modify.sql"

class Postgres:
    """Configures database"""

    sql_file_path = SQL_FILE_PATH

    @staticmethod
    def erase_db(name: str):
        """Drop a db section in Postgres and delete its configuration"""
        
        utils.run_cmds(Postgres.get_bash(f"DROP DATABASE {name}"))
        Config._remove_old_config_section(name)

##############################
### Installation Functions ###
##############################

    def install(self, section="mantis"):
        """Installs database and modifies it"""

        password = ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(24))

        Config(section).create_config(password)
        self._create_database(section, password)

    # Must delete postgres history after setting password
    @utils.delete_files("/var/lib/postgresql.psql_history")
    def _create_database(self, section: str, password: str):
        """Creates database for specific section"""

        # SQL commands to write
        sqls = [f"DROP DATABASE {section};",
                f"DROP OWNED BY {section}_user;",
                f"DROP USER {section}_user;",
                f"CREATE DATABASE {section};",
                f"CREATE USER {section}_user;",
                f"REVOKE CONNECT ON DATABASE {section} FROM PUBLIC;",
                "REVOKE ALL ON ALL TABLES IN SCHEMA public"
                " FROM {section}_user;""",
                "GRANT ALL PRIVILEGES ON DATABASE "
                f"{section} TO {section}_user;",
                "GRANT ALL PRIVILEGES ON ALL SEQUENCES "
                f"IN SCHEMA public TO {section}_user;",
                f"ALTER USER {section}_user WITH PASSWORD '{password}';",
                f"ALTER USER {section}_user WITH SUPERUSER;",
                "CREATE EXTENSION btree_gist WITH SCHEMA {section};"]

        self._run_sql_cmds(sqls)
        # Creates btree extension
        utils.run_cmds(f'sudo -u postgres psql -d {section}'
                       ' -c "CREATE EXTENSION btree_gist;"')

#####################################
### Backup and Restore Functions ####
#####################################

    @staticmethod
    def backup_table(table_name: str, section: str, file_path: str):
        """Creates a backup file of given table in the specified section"""

        cmd = ("sudo -i -u postgres "
               f"pg_dump -Fc -t {table_name} {section} > {file_path}")
        utils.run_cmds(cmd)

    @staticmethod
    def restore_table(section: str, file_path: str):
        """Restore a given table in the specified section

        Index must be re-created on the table after a restore.
        """

        cmd = ("sudo -i -u postgres "
               f"pg_restore -c --if-exists -d {section} {file_path}")
        utils.run_cmds(cmd)

    @staticmethod
    def get_bash(query: str, section:str = None):
        section_arg = f"-d {section}" if section else ""
        return f"sudo -i -u postgres psql {section_arg} -c '{query}'"

########################
### Helper Functions ###
########################

    @utils.delete_files(SQL_FILE_PATH)
    def _run_sql_cmds(self, sqls: list):
        """Writes the sql file that is later run"""

        # Writes sql file
        with open(self.sql_file_path, "w+") as _db_mod_file:
            for sql in sqls:
                assert ";" in sql, f"{sql} statement has no ;"
                _db_mod_file.write(sql + "\n")
        # Runst he sql commands
        utils.run_cmds(f"sudo -u postgres psql -f {Postgres.sql_file_path}")
