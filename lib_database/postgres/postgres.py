import logging
from multiprocessing import cpu_count

from psutil import virtual_memory

from lib_utils import file_funcs

class Postgres:
    """Handles Postgres configuration and functions"""

    # Defaults
    from .postgres_defaults import default_conf_section
    from .postgres_defaults import default_db_kwargs

    # Create database
    def create_database(self, conf_section=DEFAULT_CONF_SECTION, **kwargs):
        """Writes database entry in config. Creates database. Modifies db"""

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
        databases = check_output(self.get_sql_bash(sql), shell=True)[2:-1]
        for database in databases:
            print("Make sure this works")
            input(database)
            self.drop_database(database)

    def drop_database(self, db):
        """Drops database if exists"""

        self.run_sql_cmds(f"DROP DATABASE IF EXISTS {db};")
        self._remove_db_from_config(db)

    def _remove_db_from_config(self, db):
        """Removes all config entries that include a specific database"""

        # Open config and get dict
        with Config(write=True) as conf_dict:
            sections_to_delete = []
            # For each section in the config
            for section, section_dict in conf_dict.items():
                # If the database in that section is to be removed
                if section_dict["database"] == db:
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

        return f"sudo -u postgres psql -c {sql}"
