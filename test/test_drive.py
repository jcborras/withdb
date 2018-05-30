#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from unittest import TestCase, main, skip

import withdb


class TestDrive(TestCase):

    def test_mysql_1(self):
        self.assertIsNotNone(withdb.mysql.MySQLconnection('foo'))

    def test_psql_1(self):
        self.assertIsNotNone(withdb.psql.PostgreSQLconnection('foo'))
