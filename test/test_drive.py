#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from unittest import TestCase, main, skip

from withdb import factory


class TestDrive(TestCase):
    """Test drive all base/basic features of the module"""
    def test_logging_errors(self):
        pass
        # self.assertIsNotNone(withdb.mysql.MySQLconnection('foo'))

    def test_logging_debug(self):
        pass
        # self.assertIsNotNone(withdb.mysql.MySQLconnection('foo'))

    def test_bad_factory_params_1(self):
        """SQLite not supported just yet"""
        bad_params = {'type': 'sqlite', 'params': None}
        self.assertRaises(RuntimeError, factory, bad_params)


if __name__ == '__main__':
    main()
