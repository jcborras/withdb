#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from unittest import TestCase, main, skip

import withdb


class TestDrive(TestCase):

    def test_mysql_1(self):
        self.assertIsNotNone(withdb.mysql.MySQLconnection('foo'))

    def test_psql_1(self):
        self.assertIsNotNone(withdb.psql.PostgreSQLconnection('foo'))

    def test_factory_psql_1(self):
        cfg = {
            'type': 'psql',
            'params': {
                'a': 'b',
                'c': 6,
            }
        }
        self.assertIsNotNone(withdb.factory(cfg))
        self.assertIsInstance(withdb.factory(cfg),
                              withdb.psql.PostgreSQLconnection)

    @skip('Missing valid db user and password')
    def test_factory_psql_2(self):
        cfg = {
            'type': 'psql',
            'params': {
                'host': 'localhost',
                'port': 55432,
                'user': 'user',
                'password': 'password',
                'database': 'postgres',
            }
        }
        self.assertIsInstance(withdb.factory(cfg),
                              withdb.psql.PostgreSQLconnection)
        with withdb.factory(cfg) as conn:
            qry = "SELECT * FROM information_schema.attributes;"
            rows, cols = conn(qry)
        self.assertEqual(len(rows), 0)
        self.assertIn('attribute_name', cols)
        self.assertIn('scope_catalog', cols)

    @skip('Missing valid db user and password')
    def test_factory_psql_3(self):
        cfg = {
            'type': 'psql',
            'params': {
                'dsn': "postgresql://user:password@localhost:55432/postgres",
            }
        }
        self.assertIsInstance(withdb.factory(cfg),
                              withdb.psql.PostgreSQLconnection)
        with withdb.factory(cfg) as conn:
            qry = "SELECT * FROM information_schema.attributes;"
            rows, cols = conn(qry)
        self.assertEqual(len(rows), 0)
        self.assertIn('attribute_name', cols)
        self.assertIn('scope_catalog', cols)
