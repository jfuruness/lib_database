import itertools
import os

import numpy as np
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
    def test_clear(self, test_table, clear):
        """Tests that upon init subtable is cleared"""

        with test_table.__class__(clear=clear) as db:
            sql = f"SELECT * FROM {db.name}"
            results = db.execute(sql)
            assert (len(results) == 0) is clear

    def test_create_table(self, test_table):
        """Tests that upon init subtable is created"""

        sql = f"DROP TABLE IF EXISTS {test_table.name}"
        test_table.execute(sql)
        sql = f"SELECT * FROM {test_table.name}"
        with pytest.raises(psycopg2.errors.UndefinedTable):
            test_table.execute(sql)

        test_table.create_table()

        # If the table didn't exist this would error
        assert len(test_table.execute(sql)) == 0

    @pytest.mark.parametrize("iter_func,id_col",
                             # Cartesian product
                             # Can't have iter types in one place
                             # because np.array is list comp, but np.ndarray
                             # Is the type
                             list(itertools.product([list, tuple, np.array],
                                                    ["test_id", None])))
    def test_insert(self, iter_func, id_col):
        """Tests the insert function for the generic_table"""

        temp = id_col
        class Test_Table(GenericTable):
            """Test table class"""

            name = "test"
            id_col = temp

            def create_table(self):
                sql = f"""CREATE TABLE IF NOT EXISTS {self.name} (
                      data INTEGER[], test_val INTEGER"""
                if self.id_col:
                    sql += f", {id_col} SERIAL PRIMARY KEY "
                sql += ");"
                self.execute(sql)

        # Data to feed the test table
        data = {"data": iter_func([1, 2, 3]), "test_val": 1}
        # Create the test table
        with Test_Table(clear=True) as db:
            # Insert the data
            id_col = db.insert(data)
            # Make sure id col is correct or that it is None
            assert isinstance(id_col, int) or (db.id_col is None)
            sql = f"SELECT * FROM {db.name}"
            results = db.execute(sql)
            assert results[0]["data"] == list(data["data"])
            assert results[0]["test_val"] == data["test_val"]

    def test_get_all(self, test_table):
        """Tests get_all function"""

        test_table.match_default_rows(test_table.get_all())

    def test_get_count_no_sql_no_data(self, test_table):
        """Tests get_count with no sql or data passed in"""

        assert test_table.get_count() == len(test_table.default_rows)

    def test_get_count_sql_no_data(self, test_table):
        """Tests get_count with sql but no data as params"""

        num = 5
        assert test_table.get_count(f"SELECT {num} AS count;") == num

    def test_get_count_no_sql_data(self, test_table):
        """Tests that an assertion err is raised if data with no sql"""

        with pytest.raises(AssertionError):
            test_table.get_count(data=[5])

    def test_get_count_sql_data(self, test_table):
        sql = f"""SELECT COUNT(*) FROM {test_table.name}
                  WHERE col1 = %s AND col2 = %s"""
        row = test_table.default_rows[0]
        data = [row[k] for k in ["col1", "col2"]]
        assert test_table.get_count(sql, data) == 1

    @pytest.mark.skip(reason="Must be a superuser")
    def test_copy_to_tsv(self, test_table):
        """Tests that a table can be copied to a TSV file"""

        file_path = "/tmp/test_table.tsv"
        delete_paths(file_path)
        assert not os.path.exists(file_path)
        test_table.copy_to_tsv(file_path)
        assert os.path.exists(file_path)
        with open(file_path, "r") as f:
            assert len(f.readlines()) == len(test_table.default_rows)
            delete_paths(file_path)
