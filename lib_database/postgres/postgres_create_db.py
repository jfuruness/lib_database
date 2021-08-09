import logging
from multiprocessing import cpu_count
from subprocess import check_output

from psutil import virtual_memory

from lib_config import Config
from lib_utils import file_funcs

from .postgres_defaults import DEFAULT_CONF_SECTION

def _write_db_conf(self, conf_section, **kwargs): 
    """Writes database information in config"""

    logging.info("Writing database config")
    with Config(write=True) as conf_dict:
        if conf_section not in conf_dict:
            conf_dict[conf_section] = {}
        for k, v in self.default_db_kwargs.items():
            # Get the kwarg, or if not exists, the default_kwarg
            conf_dict[conf_section][k] = kwargs.get(k, v)
        # User is always only used for one db
        # This is because we drop and restore user for each db
        # And set password for each user for each db
        user = conf_dict[conf_section]["database"] + "_user"
        conf_dict[conf_section]["user"] = user

def _get_db_creds(self, conf_section=DEFAULT_CONF_SECTION):
    """Gets database creds"""

    with Config(write=False) as conf_dict:
        return {k: conf_dict[conf_section][k]
                for k in list(self.default_db_kwargs.keys()) + ["user"]}

def _init_db(self, user=None, database=None, host=None, password=None):
    """Creates database and user and configures it for access"""

    logging.info("Initializing database")
    sqls = [f"DROP DATABASE IF EXISTS {database};",
            #f"DROP OWNED BY {user} IF EXISTS;",
            f"DROP USER IF EXISTS {user};",
            f"CREATE DATABASE {database};",
            f"CREATE USER {user};",
            f"REVOKE CONNECT ON DATABASE {database} FROM PUBLIC;",
            f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {user};",
            f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {user};",
            "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public"
            f" TO {user};",
            f"ALTER USER {user} WITH PASSWORD '{password}';",
            f"ALTER USER {user} WITH SUPERUSER;"]

    self.run_sql_cmds(sqls)
    file_funcs.delete_paths("/var/lib/postgresql.psql_history")

def _modify_db(self, db=None, ram=None, cpus=cpu_count() - 1, ssd=True):
    """Modifies database for speed.

    The database will be corrupted if there is a crash. These changes
    work at a cluster level, so all databases will be changed. This is
    also meant to maximize the database for the server, so other things
    will run slower and have less RAM/cache.
    """

    logging.info("Modifying db for speed")
    ram = ram if ram else self._get_ram()
    random_page_cost = 1 if ssd else 2

    sqls = [f"ALTER DATABASE {db} SET timezone TO 'UTC';",
            # These are settings that ensure data isn't corrupted in
            # the event of a crash. We don't care so...
            "ALTER SYSTEM SET fsync TO off;",
            "ALTER SYSTEM SET synchronous_commit TO off;",
            "ALTER SYSTEM SET full_page_writes TO off;",

            # Allows for parallelization
            f"ALTER SYSTEM SET max_parallel_workers_per_gather TO {cpus};",
            f"ALTER SYSTEM SET max_parallel_workers TO {cpus};",
            f"ALTER SYSTEM SET max_worker_processes TO {cpu_count() * 2};",

            # Writes as few logs as possible
            "ALTER SYSTEM SET wal_level TO minimal;",
            "ALTER SYSTEM SET archive_mode TO off;",
            "ALTER SYSTEM SET max_wal_senders TO 0;",

            # https://www.postgresql.org/docs/current/
            # runtime-config-resource.html
            # https://dba.stackexchange.com/a/18486
            # https://severalnines.com/blog/
            # setting-optimal-environment-postgresql
            # Buffers for postgres, set to 40%, and no more
            f"ALTER SYSTEM SET shared_buffers TO '{int(.4 * ram)}MB';",
            # Memory per process, since 11 paralell gathers and
            # some for vacuuming, set to ram/(1.5*cores)
            "ALTER SYSTEM SET work_mem TO "
            f"'{int(ram / (cpu_count() * 1.5))}MB';",
            # Total cache postgres has, ignore shared buffers
            f"ALTER SYSTEM SET effective_cache_size TO '{ram}MB';",
            # Set random page cost to 2 if no ssd, with ssd
            # seek time is one for ssds
            f"ALTER SYSTEM SET random_page_cost TO {random_page_cost};",
            # Yes I know I could call this, but this is just for machines
            # that might not have it or whatever
            # Gets the maximum safe depth of a servers execution stack
            # in kilobytes from ulimit -s
            # https://www.postgresql.org/docs/9.1/runtime-config-resource.html
            # Subtract one megabyte for safety
            "ALTER SYSTEM SET max_stack_depth TO "
            f"'{self._get_ulimit() - 1000}kB';"]
    self.run_sql_cmds(sqls)
    self.restart_postgres()

def _get_ram(self):
    # Returns RAM in megabytes
    return virtual_memory().available * .9 // 1000000

def _get_ulimit(self):
    # What ulimit -s returns: https://superuser.com/a/220064
    return int(check_output("ulimit -s", shell=True).decode().strip())
