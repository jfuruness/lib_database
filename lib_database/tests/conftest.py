import pytest

from ..generic_table import GenericTable


@pytest.fixture(scope="function")
def TestTable():
    """Creates a test table. Adds 1 row. Yields. Deletes."""

    class TestTable(GenericTable):
        name = "test"
        id_col = None

        default_rows = [{"col1": 0, "col2": 0},
                        {"col1": 1, "col2": 1}]

        def __init__(self, *args, **kwargs):
            # Assert uniqueness in rows. Useful for later in pytest
            # Other code is dependent on this uniqueness. DO NOT CHANGE.
            for key in ["col1", "col2"]:
                all_values = [x[key] for x in self.default_rows]
                assert len(all_values) == len(set(all_values))
            super(TestTable, self).__init__(*args, **kwargs)

        def create_table(self):
            sql = f"""CREATE TABLE {self.name}(
                    col1 INTEGER, col2 INTEGER
                  );"""
            self.execute(sql)

        def fill_table(self):
            for row in self.default_rows:
                self.insert(row)

        @staticmethod
        def match_default_rows(rows):
            for db_row in [{k: v for k, v in row.items()} for row in rows]:
                assert db_row in TestTable.default_rows

    table = TestTable(clear=True)
    table.fill_table()
    yield table
    table.clear_table()
    table.close()
