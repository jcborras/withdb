#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

__author__ = 'Juan Carlos Borrás'
__version__ = '0.0.4'

from datetime import datetime
from logging import getLogger, getLevelName

from withdb.mysql import MySQLconnection
from withdb.psql import PostgreSQLconnection

logger = getLogger(__name__)
logger.setLevel(getLevelName('CRITICAL') + 100)


def factory(params):
    logger.debug(params)
    try:
        return {
            'mysql': MySQLconnection,
            'psql': PostgreSQLconnection,
        }[params['type']](params['params'])
    except KeyError:
        msg = 'Bad connection type "{t:s}"'.format(t=params['type'])
        logger.error(msg)
        raise RuntimeError(msg)


def run_select(cfg, qry):
    """Runs a SQL query and returns the results as a list of dicts.
    It doesn't have to be a SELECT query but it'll fail otherwise
    """
    assert "SELECT" in qry[:7], "I rather run a SELECT query"
    with factory(cfg) as dbconn:
        t0 = datetime.now()
        logger.debug(qry)
        rows, colnames = dbconn(qry)
        d = datetime.now() - t0
        _ = 'Extraction completed after {d:.2f} s.'.format(d=d.total_seconds())
        logger.info(_)
    return [dict(zip(tuple(colnames), i)) for i in rows], colnames
