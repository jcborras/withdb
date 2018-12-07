#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

"""Test drive all base/basic features of the module"""

from io import StringIO
from logging import Formatter, StreamHandler, getLevelName, getLogger
from unittest import TestCase, main, skip

from sundry import logger_output_for
from withdb import factory

FMT = '%(asctime)s %(name)s %(filename)s:%(lineno)d '
FMT += '%(levelname)s: %(funcName)s: %(message)s'


class TestDrive(TestCase):
    def test_logging_errors(self):
        """ERROR logs are retrieved"""
        iostr = logger_output_for(logger_name='withdb', logging_level='ERROR')
        bad_params = {'type': 'sqlite', 'params': None}
        self.assertRaises(RuntimeError, factory, bad_params)
        _ = iostr.getvalue()
        self.assertEqual(len(_.splitlines()), 1)
        self.assertRegex(_, 'withdb __init__.py:\\d+ ERROR', _)

    def test_logging_debug(self):
        """Up to DEBUG logs are retrieved"""
        iostr = logger_output_for(logger_name='withdb', logging_level='DEBUG')
        bad_params = {'type': 'sqlite', 'params': None}
        self.assertRaises(RuntimeError, factory, bad_params)
        _ = iostr.getvalue()
        self.assertEqual(len(_.splitlines()), 2)
        self.assertRegex(_, 'withdb __init__.py:\\d+ DEBUG', _)
        self.assertRegex(_, 'withdb __init__.py:\\d+ ERROR', _)

    def test_bad_factory_params_1(self):
        """SQLite not supported just yet"""
        iostr = logger_output_for(logger_name='withdb', logging_level='ERROR')
        bad_params = {'type': 'sqlite', 'params': None}
        self.assertRaises(RuntimeError, factory, bad_params)
        _ = [i for i in iostr.getvalue().splitlines() if 'ERROR' in i]
        self.assertRegex(_[0], 'factory: Bad connection type')


if __name__ == '__main__':
    main()
