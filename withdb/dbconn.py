#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from datetime import datetime
from logging import getLogger, getLevelName

logger = getLogger(__name__)
logger.setLevel(getLevelName('CRITICAL'))


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
        logger.debug(query)
        t0 = datetime.now()
        self.cur.execute(query)
        t = datetime.now() - t0
        _ = 'Query execution time {t:.2f} s.'.format(t=t.total_seconds())
        logger.info(_.format(t=t.total_seconds()))
        colnames, tuples = None, None
        if self.cur.description:
            colnames = [i[0] for i in self.cur.description]
            tuples = [i for i in self.cur]
        return tuples, colnames

    def nrows(self, tablename):
        qry = "SELECT COUNT(*) AS n FROM {t:s};".format(t=tablename)
        rows, cols = self.__call__(qry)
        n = rows[0][0]
        logger.info('Row count: {n:d}'.format(n=n))
        return n

    def bulkload(self, tablename, filename):
        msg = 'Please implement bulkload() on all descendents'
        logger.error(msg)
        raise RuntimeError(msg)

    def bulkload_lod(self, lod, keys, tablename, tmp_prefix='/tmp/'):
        msg = 'Please implement bulkload_lod() on all descendents'
        logger.error(msg)
        raise RuntimeError(msg)
