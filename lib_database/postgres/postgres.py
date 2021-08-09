import logging
from multiprocessing import cpu_count
from subprocess import check_output, CalledProcessError

from psutil import virtual_memory

from lib_config import Config
from lib_utils import file_funcs, helper_funcs

from .postgres_defaults import DEFAULT_CONF_SECTION

class Postgres:
    """Handles Postgres configuration and functions"""

    # Defaults
    from .postgres_defaults import default_conf_section
    from .postgres_defaults import default_db_kwargs

    # Create database
    def create_database(self, conf_section=DEFAULT_CONF_SECTION, **kwargs):
        """Writes database entry in config. Creates database. Modifies db"""

        database = kwargs.get("database", self.default_db_kwargs["database"])
        self.drop_database(database)
        self._write_db_conf(conf_section, **kwargs)
        self._init_db(**self._get_db_creds(conf_section))
        self._modify_db(db=self._get_db_creds(conf_section)["database"])

    # Create database helpers
    from .postgres_create_db import _write_db_conf
    from .postgres_create_db import _get_db_creds
    from .postgres_create_db import _init_db
    from .postgres_create_db import _modify_db
    from .postgres_create_db import _get_ram
    from .postgres_create_db import _get_ulimit

    @staticmethod
    def restart_postgres():
        logging.info("Restarting Postgres")
        helper_funcs.run_cmds("sudo systemctl restart postgresql")
        logging.debug("Postgres restart complete")

    def drop_all_databases(self):
        """Drops all databases that exist"""

        sql = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        databases = check_output(self._get_sql_bash(sql), shell=True)
        databases = databases.decode().split("\n")[2:-3]
        for database in databases:
            if "postgres" not in database:
                self.drop_database(database.strip())

    def drop_database(self, db_name: str):
        """Drops database if exists"""

        try:
            self._terminate_db_connections(db_name)
        # This happens every time a conn is closed, so we ignore
        except CalledProcessError as e:
            pass
        self.run_sql_cmds([f"DROP DATABASE IF EXISTS {db_name};"])
        self._remove_db_from_config(db_name)

    def _terminate_db_connections(self, db_name: str):
        """Closes all connections to a database"""

        sql1 = f"REVOKE CONNECT ON DATABASE {db_name} FROM PUBLIC;"
        sql2 = f"""select pg_terminate_backend(pid)
                from pg_stat_activity where datname='{db_name}';"""
        self.run_sql_cmds([sql1, sql2])


    def _remove_db_from_config(self, db):
        """Removes all config entries that include a specific database"""

        # Open config and get dict
        with Config(write=True) as conf_dict:
            sections_to_delete = []
            # For each section in the config
            for section, section_dict in conf_dict.items():
                # If the database in that section is to be removed
                if section_dict.get("database") == db:
                    # Save that section
                    sections_to_delete.append(db)
            # Delete all sections we no longer need
            for section_to_delete in sections_to_delete:
                del conf_dict[section_to_delete]

    def run_sql_cmds(self, sqls: list):
        """Runs SQL commands"""

        assert isinstance(sqls, list), "Must be a list of SQL commands"
        for sql in sqls:
            assert sql[-1] == ";", f"{sql} statement has no ;"
            helper_funcs.run_cmds(self._get_sql_bash(sql))

    def _get_sql_bash(self, sql):
        """Returns SQL turned into bash"""

        return f'sudo -u postgres psql -c "{sql}"'
