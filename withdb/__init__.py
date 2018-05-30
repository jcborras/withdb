#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

__author__ = 'Juan Carlos Borrás'
__version__ = '0.0.2'

from withdb.mysql import MySQLconnection
from withdb.psql import PostgreSQLconnection


def factory(params):
    try:
        return {
            'mysql': MySQLconnection,
            'psql': PostgreSQLconnection,
        }[params['type']](params['params'])
    except KeyError:
        s = 'Bad connection type "{t:s}'
        raise RuntimeError(s.format(t=params['type']))


def run_select(cfg, qry, logger=None):
    """Runs a SQL query and returns the results as a list of dicts.
    It doesn't have to be a SELECT query but it'll fail otherwise
    """
    assert "SELECT" in qry[:7], "I rather run a SELECT query"
    with factory(cfg) as dbconn:
        t0 = datetime.now()
        rows, colnames = dbconn(qry)
        d = datetime.now() - t0
        if logger:
            logger.info('Extraction completed after {d:.2f} s.'.format(
                d=d.total_seconds()))
    return [dict(zip(tuple(colnames), i)) for i in rows], colnames
