from __future__ import absolute_import

from ps_parser import PandasSqlParser
import gc


class ExcelCursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1
        self.fetched_rows = 0
        self._context = None
        self.result = None
        self.description = None

    def _extract_description(self):
        column_meta = []
        for column in self.result:
            column_meta.append(
                [column, None, None, 0, None, 0, True]
            )
        return column_meta

    def close(self):
        del self.result
        gc.collect()

    def fetchone(self):
        if self.fetched_rows < self.rowcount:
            row = self.result[self.fetched_rows]
            self.fetched_rows += 1
            return row
        else:
            return None

    def fetchmany(self, size=None):
        fetched_rows = self.fetched_rows
        self.fetched_rows += size
        return self.result[fetched_rows:self.fetched_rows]

    def fetchall(self):
        fetched_rows = self.fetched_rows
        self.fetched_rows = self.rowcount
        return self.result[fetched_rows:]

    def execute(self, operation, parameters={}):
        sql = operation % parameters
        psp = PandasSqlParser(sql)
        table_names = set()
        for db_name, tb_name in psp.source_tables(True):
            table_names.add(tb_name)
        context = self.connection.load_all_table(table_names)
        self.result = psp.execute(context)
        for df in context.values():
            del df
        gc.collect()
        self.description = self._extract_description()
        self.rowcount = len(self.result)
        self.fetched_rows = 0
        self.result = self.result.values
        return self.rowcount

    def executemany(self, operation, seq_params=[]):
        result = []
        for param in seq_params:
            self.execute(operation, param)
            result.extend(self.result)
        self.result = result
        self.rowcount = len(self.result)
        self.fetched_rows = 0
        return self.rowcount
