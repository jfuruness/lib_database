from psycopg2.extras import NamedTupleCursor
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

    def test_default_database(self, TestTable):
        """Tests connection with the default database"""

        with Database() as db:
            rows = db.execute(f"SELECT * FROM {TestTable.name}")
            assert rows = len(TestTable.default_rows)

    def test_non_default_database(self, TestTable):
        """Tests connection with non default database"""

        og_default = Database.default_database
        Database.default_database = "non_default"
        with Database(database=og_default) as db:
            rows = db.execute(f"SELECT * FROM {TestTable.name}")
            assert rows = len(TestTable.default_rows)
        Database.default_database = og_default

    def test_invalid_database(self):
        """Tests connection with invalid database"""

        with Database(database="invalid") as db:
            assert False, "Figure out this error then assert it occurs"

    def test_execute_no_data_yes_return(self, TestTable):
        """Tests the execute function with no data and expects return"""

        with Database() as db:
            rows = db.execute(f"SELECT * FROM {TestTable.name}")
            assert len(rows) == len(TestTable.default_rows)

    def test_execute_yes_data_yes_return(self, TestTable):
        """Tests the execute function with data and expects return"""

        with Database() as db:
            sql = f"SELECT * FROM {TestTable.name} WHERE col1 = %s"
            rows = db.execute(sql, TestTable.default_rows[0]["col1"])
            # Test Table rows are unique
            assert len(rows) == 1

    def test_execute_no_data_no_return(self):
        """Tests the execute function with no data and expects no return"""

        with Database() as db:
            sql = f"SELECT * FROM {TestTable.name} WHERE col1 = 0 AND col1 = 1"
            rows = db.execute(sql)
            assert len(rows) == 0


    def test_execute_yes_data_no_return(self):
        """Tests the execute function with data and expects no return"""

        with Database() as db:
            sql = f"""SELECT * FROM {TestTable.name}
                  WHERE col1 = %s AND col1 = %s"""
            rows = db.execute(sql, [0, 1])
            assert len(rows) == 0

    def test_cursor_factory(self, TestTable):
        """Tests that the cursor factor argument can be set"""

        with Database(cursor_factory=NamedTupleCursor) as db:
            rows = db.execute(f"SELECT * FROM {TestTable.name}")
            for row in rows:
                print(row)
                print(type(row))
                assert False, "Make sure this is a named tuple and not a dict"
            
