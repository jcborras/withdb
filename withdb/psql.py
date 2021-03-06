#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from csv import DictWriter, QUOTE_NONNUMERIC, register_dialect
from datetime import datetime
from logging import getLogger, getLevelName
from re import sub

from psycopg2 import connect as psyconnect
from psycopg2 import ProgrammingError as psyProgrammingError

from withdb.dbconn import DbConnection

head, *tail = __name__.split('.')
logger = getLogger(head)
logger.setLevel(getLevelName('CRITICAL'))


class PostgreSQLconnection(DbConnection):
    def __init__(self, params):
        super().__init__(psyconnect, params)

    def __call__(self, query):
        try:
            return super().__call__(query)
        except psyProgrammingError as e:
            logger.error(e.pgerror)
            _ = [i for i in dir(e.diag) if not i.startswith('__')]
            _ = [(i, e.diag.__getattribute__(i)) for i in _]
            for i in _:
                logger.error('   ' + str(i[0]) + ': ' + str(i[1]))
            raise RuntimeError(e.pgerror)
        except BaseException as e:
            raise RuntimeError(e.args)

    def bulkload(self, tablename, filename):
        tplt = "COPY {t:s} FROM '{f:s}' DELIMITER ',' CSV;"
        qry = tplt.format(f=filename, t=tablename)
        logger.debug(qry)
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


if __name__ == '__main__':
    assert PostgreSQLconnection('foo') is None, "Instantiation was not OK"
