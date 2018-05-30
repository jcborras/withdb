#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

# http://effbot.org/zone/python-with-statement.htm

from csv import DictWriter, QUOTE_NONNUMERIC, register_dialect
from datetime import datetime
from re import sub

from mysql.connector import connect as mysql_connect
from mysql.connector.errors import ProgrammingError as MySQLProgrammingError
from psycopg2 import connect as psyconnect
from psycopg2 import ProgrammingError as psyProgrammingError


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


class MySQLconnection(DbConnection):
    def __init__(self, params):
        super().__init__(mysql_connect, params)

    def __call__(self, query):
        try:
            return super().__call__(query)
        except MySQLProgrammingError as e:
            raise RuntimeError(e.args[1])
        except BaseException as e:
            raise RuntimeError(e.args[0])

    def bulkload(self, tablename, filename):
        qry = """LOAD DATA INFILE '{f:s}' INTO TABLE {t:s}
        CHARACTER SET 'utf8' FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n';"""
        self.__call__(qry.format(f=filename, t=tablename))

    def bulkload_lod(self, lod, keys, tablename, tmp_prefix='/tmp/'):
        filename = tmp_prefix + datetime.now().strftime("%s") + '.csv'
        register_dialect('own', 'excel', delimiter=',', lineterminator='\n',
                         quoting=QUOTE_NONNUMERIC)
        with open(filename, 'w', encoding='utf-8') as f:
            wr = DictWriter(f, keys, dialect='own')
            wr.writerows(lod)
#        with open(filename, 'rt', encoding='utf-8') as f:
#            lines = f.readlines()
#            sublines = [sub('""', '', i) for i in lines]
#        with open(filename, 'w', encoding='utf-8') as f:
#            f.writelines(sublines)
        self.bulkload(tablename, filename)
        return filename


class PostgreSQLconnection(DbConnection):
    def __init__(self, params):
        super().__init__(psyconnect, params)

    def __call__(self, query):
        try:
            return super().__call__(query)
        except psyProgrammingError as e:
            # print(dir(e.diag)) # TODO: nice object for detailed error report
            raise RuntimeError(e.pgerror)
        except BaseException as e:
            raise RuntimeError(e.args)

    def bulkload(self, tablename, filename):
        tplt = "COPY {t:s} FROM '{f:s}' DELIMITER ',' CSV;"
        qry = tplt.format(f=filename, t=tablename)
        self.__call__(qry)

    def bulkload_lod(self, lod, keys, tablename, tmp_prefix='/tmp/'):
        filename = tmp_prefix + datetime.now().strftime("%s") + '.csv'
        register_dialect('own', 'excel', delimiter=',', lineterminator='\n',
                         quoting=QUOTE_NONNUMERIC)
        with open(filename, 'w', encoding='utf-8') as f:
            wr = DictWriter(f, keys, dialect='own')
            wr.writerows(lod)
        with open(filename, 'rt', encoding='utf-8') as f:
            lines = f.readlines()
            sublines = [sub('""', '', i) for i in lines]
        with open(filename, 'w', encoding='utf-8') as f:
            f.writelines(sublines)
        self.bulkload(tablename, filename)
        return filename


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
