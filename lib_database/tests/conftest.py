import pytest

from ..database import GenericTable


@pytest.fixture(scope="function")
def TestTable():
    """Creates a test table. Adds 1 row. Yields. Deletes."""

    class TestTable(GenericTable):
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
            sql = """CREATE TABLE test_table(
                    col1 INTEGER, col2 INTEGER
                  );"""
            db.execute(sql)
        def fill_table(self):
            for row in self.default_rows:
                self.insert(row)

    table = TestTable(clear=True)
    yield table
    table.close()
