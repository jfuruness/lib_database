import psycopg2.extras
import pytest

from ..database import Database


@pytest.mark.database
class TestDatabase:
    """Tests the wrapper around the database connections"""

    def test_init_close_defaults(self):
        """Tests the init, close funcs with defaults"""

        db = Database()
        db.close()

    def test_context_manager(self):
        """Tests init of db with context manager"""

        with Database():
            pass

    def test_default_database(self, test_table):
        """Tests connection with the default database"""

        with Database() as db:
            rows = db.execute(f"SELECT * FROM {test_table.name}")
            test_table.match_default_rows(rows)

    def test_non_default_database(self, test_table):
        """Tests connection with non default database"""

        og_default = Database.default_database
        Database.default_database = "non_default"
        with Database(database=og_default) as db:
            rows = db.execute(f"SELECT * FROM {test_table.name}")
            test_table.match_default_rows(rows)
        Database.default_database = og_default

    @pytest.mark.skip(reason="Come back to later for clearer err handling")
    def test_db_not_in_config(self):
        """Tests connection with no database in config"""

        pass

    @pytest.mark.skip(reason="Come back to later for clearer err handling")
    def test_config_creds_wrong(self):
        """Tests connection with incorrect creds in config"""

        pass

    @pytest.mark.skip(reason="Come back to later for clearer err handling")
    def test_config_partial_creds(self):
        """Tests connection with partial creds in config"""

        pass

    @pytest.mark.skip(reason="Come back to later for clearer err handling")
    def test_config_database_does_not_exist(self):
        """Tests connection with config section but no database"""

        pass

    def test_execute_no_data_yes_return(self, test_table):
        """Tests the execute function with no data and expects return"""

        with Database() as db:
            rows = db.execute(f"SELECT * FROM {test_table.name}")
            test_table.match_default_rows(rows)

    def test_execute_yes_data_yes_return(self, test_table):
        """Tests the execute function with data and expects return"""

        with Database() as db:
            sql = f"SELECT * FROM {test_table.name} WHERE col1 = %s"
            rows = db.execute(sql, [test_table.default_rows[0]["col1"]])
            # Test Table rows are unique
            assert len(rows) == 1

    def test_execute_no_data_no_return(self, test_table):
        """Tests the execute function with no data and expects no return"""

        with Database() as db:
            sql = f"SELECT * FROM {test_table.name} WHERE col1 = 0 AND col1 = 1"
            rows = db.execute(sql)
            assert len(rows) == 0

    def test_execute_yes_data_no_return(self, test_table):
        """Tests the execute function with data and expects no return"""

        with Database() as db:
            sql = f"""SELECT * FROM {test_table.name}
                  WHERE col1 = %s AND col1 = %s"""
            rows = db.execute(sql, [0, 1])
            assert len(rows) == 0

    def test_cursor_factory(self, test_table):
        """Tests that the cursor factor argument can be set"""

        with Database() as db:
            real_dict_rows = db.execute(f"SELECT * FROM {test_table.name}")

        with Database(cursor_factory=psycopg2.extras.NamedTupleCursor) as db:
            named_tuple_rows = db.execute(f"SELECT * FROM {test_table.name}")
            # There doesn't seem to be a way to access the named tuple class
            # Because of this, we can just check to make sure it's not equal
            assert type(real_dict_rows[0]) != type(named_tuple_rows[0])
