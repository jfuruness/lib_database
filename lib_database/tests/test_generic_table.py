import itertools
import os

import psycopg2
import pytest

from lib_utils.file_funcs import delete_paths

from ..generic_table import GenericTable


@pytest.mark.generic_table
class TestGenericTable:
    """Tests the wrapper around the database connections for tables"""

    def test_no_name(self):
        """Tests subclassing with no name"""

        class Subtable(GenericTable):
            id_col = None

        with pytest.raises(AssertionError):
            Subtable()

    def test_no_id_col(self):
        """Tests subclassing with no id column"""

        class Subtable(GenericTable):
            name = "test"

        with pytest.raises(AssertionError):
            Subtable()

    @pytest.mark.parametrize("clear", [True, False])
    def test_clear(self, TestTable, clear):
        """Tests that upon init subtable is cleared"""

        with TestTable(clear=True) as db:
            db.fill_table()

        with TestTable(clear=clear) as db:
            sql = f"SELECT * FROM {db.name}"
            results = db.execute(sql)
            assert (len(results) == 0) is clear

    def test_create_table(self, TestTable):
        """Tests that upon init subtable is created"""

        with TestTable(clear=True) as db:
            sql = f"DROP TABLE IF EXISTS {db.name}"
            db.execute(sql)
            sql = f"SELECT * FROM {db.name}"
            with pytest.raises(psycopg2.UndefinedTableException):
                db.execute(sql)

            with TestTable():
                pass

            # If the table didn't exist this would error
            assert len(db.execute(sql)) == 0

    @pytest.mark.parametrize("iter_func,id_col",
                             # Cartesian product
                             list(itertools.product(GenericTable.iter_types(),
                                                    ["test_id", None])))
    def test_insert(self, iter_func, id_col):
        """Tests the insert function for the generic_table"""

        class TestTable(GenericTable):
            """Test table class"""

            name = "test"
            id_col = id_col

            def create_table(self):
                sql = f"""CREATE TABLE IF NOT EXISTS {self.name} (
                      data INTEGER[], test_val INTEGER"""
                if self.id_col:
                    sql += f", {id_col} SERIAL PRIMARY KEY "
                sql += ");"
                db.execute(sql)

        # Data to feed the test table
        data = {"data": iter_func([1, 2, 3]), "test_val": 1}
        # Create the test table
        with TestTable(clear=True) as db:
            # Insert the data
            id_col = db.insert(data)
            # Make sure id col is correct or that it is None
            assert isinstance(id_col, int) or (db.id_col is None)
            sql = f"SELECT * FROM {db.name}"
            results = db.execute(sql)
            assert results[0]["data"] == list(data["data"])
            assert results[0]["test_val"] == data["test_val"]

    def test_get_all(self, TestTable):
        """Tests get_all function"""

        with TestTable(clear=True) as db:
            db.fill_table()
            TestTable.match_default_rows(db.get_all())

    def test_get_count_no_sql_no_data(self, TestTable):
        """Tests get_count with no sql or data passed in"""

        with TestTable(clear=True) as db:
            assert db.get_count() == 0
            db.fill_table()
            assert db.get_count() == len(db.default_rows)

    def test_get_count_sql_no_data(self, TestTable):
        """Tests get_count with sql but no data as params"""

        with TestTable(clear=True) as db:
            db.fill_table()
            num = 5
            assert db.get_count(f"SELECT {num} FROM {db.name}") == num

    def test_get_count_no_sql_data(self, TestTable):
        """Tests that an assertion err is raised if data with no sql"""

        with TestTable(clear=True) as db:
            db.fill_table()
            with pytest.raises(AssertionError):
                db.get_count(data=[5])

    def test_get_count_sql_data(self, TestTable):
        with TestTable(clear=True) as db:
            db.fill_table()
            sql = f"""SELECT * FROM {TestTable.name}
                    WHERE col1 = %s
                        AND col2 = %s"""
            row = db.default_rows[0]
            data = [row[k] for k in ["col1", "col2"]]
            assert db.get_count(sql, data) == 1

    def test_copy_to_tsv(self, TestTable):
        """Tests that a table can be copied to a TSV file"""

        with TestTable(clear=True) as db:
            db.fill_table()
            file_path = "/tmp/test_table.tsv"
            delete_paths(file_path)
            assert not os.path.exists(file_path)
            db.copy_to_tsv(file_path)
            assert os.path.exists(file_path)
            with open(file_path, "r") as f:
                assert len(f.readlines()) == len(db.default_rows)
            delete_paths(file_path)
