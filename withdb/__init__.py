#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

__author__ = 'Juan Carlos Borrás'
__version__ = '0.0.4'

from datetime import datetime
from logging import getLogger, getLevelName

from withdb.mysql import MySQLconnection
from withdb.psql import PostgreSQLconnection

logger = getLogger(__name__)
logger.setLevel(getLevelName('CRITICAL'))


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
    logger.debug(qry)
    assert "SELECT" in qry[:7], "I rather run a SELECT query"
    with factory(cfg) as dbconn:
        t0 = datetime.now()
        rows, colnames = dbconn(qry)
        t = datetime.now() - t0
        _ = 'SELECT query completed after {t:.2f} s.'
        _ = _.format(t=t.total_seconds())
        logger.info(_)
    return [dict(zip(tuple(colnames), i)) for i in rows], colnames
