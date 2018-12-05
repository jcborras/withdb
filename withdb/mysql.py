#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from csv import DictWriter, QUOTE_NONNUMERIC, register_dialect
from datetime import datetime
from logging import getLogger, getLevelName
from re import sub

from mysql.connector import connect as mysql_connect
from mysql.connector.errors import ProgrammingError as MySQLProgrammingError

from withdb.dbconn import DbConnection

# The logger name must abide to the module logger hierarchy hence
# withdb -> withdb.dbconn -> withdb.dbconn.mysql
logger = getLogger(__name__.replace('.', '.dbconn.'))
logger.setLevel(getLevelName('DEBUG'))


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


if __name__ == '__main__':
    assert MySQLconnection('foo') is None, "Instantiation was not OK"
