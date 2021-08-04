import csv

def rows_to_db(list_of_dicts, Table, clear_table=True):
    with temp_path(path_append=".tsv") as path:
        with Table() as db:
            cols = db.columns
            write_dicts_to_tsv(list_of_dicts, columns, path)
        tsv_to_db(Table, path, clear_table=clear_table)

def write_dicts_to_tsv(list_of_dicts, cols, path):
    """Writes list of dicts to path"""

    with open(path, mode="w") as f:
        writer = csv.DictWriter(f, fieldnames=cols, delimter="\t")
        writer.writerow(list_of_dicts)

def tsv_to_db(Table, path, clear_table=True):
    """Writes a TSV file to the db"""

    with Table(clear=clear_table) as db:
        with open(path) as f:
            db.cursor.copy_from(f, db.name, sep="\t", null="")
