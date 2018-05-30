#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-


class DbConnection(object):
    def __init__(self, connect_function, params):
        self.connect = connect_function
        self.params = params

    def __enter__(self):
        self.dbcon = self.connect(**self.params)
        self.dbcon.autocommit = True
        self.cur = self.dbcon.cursor()
        return self

    def __exit__(self, type, value, traceback):
        self.cur.close()
        self.dbcon.close()

    def __call__(self, query):
        self.cur.execute(query)
        colnames, tuples = None, None
        if self.cur.description:
            colnames = [i[0] for i in self.cur.description]
            tuples = [i for i in self.cur]
        return tuples, colnames

    def nrows(self, tablename):
        qry = "SELECT COUNT(*) AS n FROM {t:s};".format(t=tablename)
        rows, cols = self.__call__(qry)
        return rows[0][0]

    def bulkload(self, tablename, filename):
        raise RuntimeError('Please implement bulkload() on all descendents')

    def bulkload_lod(self, lod, keys, tablename, tmp_prefix='/tmp/'):
        raise RuntimeError('Please implement bulkload() on all descendents')
