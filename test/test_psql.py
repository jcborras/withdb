#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

from datetime import datetime, timedelta, timezone
from json import load
from os.path import expanduser
from unittest import TestCase, main, skip

from pytz import timezone

import withdb


class TestDrive(TestCase):
    CFG_FILE = expanduser('~/.withdb_test_psql.cfg')

    def setUp(self):
        with open(self.CFG_FILE, 'r') as f:
            self.cfg = load(f)

    def test_00_psql(self):
        self.assertIsNotNone(withdb.psql.PostgreSQLconnection('foo'))

    def test_10_open_config_file(self):
        self.assertIsNotNone(self.cfg)
        self.assertEqual(self.cfg['type'], 'psql')

    def test_20_factory_psql_1(self):
        self.assertIsNotNone(withdb.factory(self.cfg))
        self.assertIsInstance(withdb.factory(self.cfg),
                              withdb.psql.PostgreSQLconnection)

    def test_30_factory_psql_and_select(self):
        """Check that a SELECT query runs fine"""
        self.assertIsInstance(withdb.factory(self.cfg),
                              withdb.psql.PostgreSQLconnection)
        with withdb.factory(self.cfg) as conn:
            qry = "SELECT * FROM information_schema.attributes;"
            rows, cols = conn(qry)
        self.assertEqual(len(rows), 0)
        self.assertIn('attribute_name', cols)
        self.assertIn('scope_catalog', cols)

    def test_31_factory_psql_and_select_is_logger(self):
        """Check that a SELECT query runs fine and logged as DEBUG"""
        self.assertIsInstance(withdb.factory(self.cfg),
                              withdb.psql.PostgreSQLconnection)
        with withdb.factory(self.cfg) as conn:
            qry = "SELECT * FROM information_schema.attributes;"
            rows, cols = conn(qry)
        self.assertEqual(len(rows), 0)
        self.assertIn('attribute_name', cols)
        self.assertIn('scope_catalog', cols)

    def test_40_create_table(self):
        with withdb.factory(self.cfg) as conn:
            create_qry = """
            CREATE TABLE on_timezones (
            entry_id INT NOT NULL PRIMARY KEY,
            tz_at_insert VARCHAR(24) DEFAULT CURRENT_SETTING('TIMEZONE'),
            ts_without_tz timestamp without time zone,
            ts_with_tz timestamp with time zone
            );"""
            conn(create_qry)
        with withdb.factory(self.cfg) as conn:
            rows, cols = conn("SELECT COUNT(*) AS row_count FROM on_timezones")
        self.assertEqual(rows[0], (0,))

    def test_50_drop_table(self):
        with withdb.factory(self.cfg) as conn:
            conn("DROP TABLE IF EXISTS on_timezones")
        with withdb.factory(self.cfg) as conn:
            self.assertRaises(
                RuntimeError, conn,
                "SELECT COUNT(*) AS row_count FROM on_timezones")

    def test_60_load_lod(self):
        utc_tz = timezone('UTC')
        hel_tz = timezone('Europe/Helsinki')
        cph_tz = timezone('Europe/Copenhagen')
        NOW = datetime.utcnow()
        # timezone.localize() just adds a timezone to a datetime w/o tz
        lod = [
            {
                'entry_id': 101,
                'tz_at_insert': 'Etc/UTC',
                'ts_without_tz': utc_tz.localize(NOW),
                'ts_with_tz':  utc_tz.localize(NOW),
            },
            {
                'entry_id': 102,
                'tz_at_insert': 'Europe/Helsinki',
                'ts_without_tz': hel_tz.localize(NOW),
                'ts_with_tz':  hel_tz.localize(NOW),
            },
            {
                'entry_id': 103,
                'tz_at_insert': 'Europe/Copenhagen',
                'ts_without_tz': cph_tz.localize(NOW),
                'ts_with_tz':  cph_tz.localize(NOW),
            },
        ]
        KEYS = ['entry_id', 'tz_at_insert', 'ts_without_tz', 'ts_with_tz']
        for i in lod:
            # print(i)
            self.assertEqual(
                i['ts_without_tz'], i['ts_with_tz'].astimezone(utc_tz))

        with withdb.factory(self.cfg) as conn:
            create_qry = """
            CREATE TABLE on_timezones (
            entry_id INT NOT NULL PRIMARY KEY,
            tz_at_insert VARCHAR(24) DEFAULT CURRENT_SETTING('TIMEZONE'),
            ts_without_tz timestamp without time zone,
            ts_with_tz timestamp with time zone
            );"""
            conn(create_qry)
        with withdb.factory(self.cfg) as conn:
            rows, cols = conn("SELECT COUNT(*) AS row_count FROM on_timezones")
        self.assertEqual(rows[0], (0,))

        with withdb.factory(self.cfg) as conn:
            _ = ['entry_id', 'tz_at_insert', 'ts_without_tz', 'ts_with_tz']
            conn.bulkload_lod(
                lod, _, 'on_timezones', self.cfg['tmp_dir'] + '/test_psql_')
        with withdb.factory(self.cfg) as conn:
            rows, cols = conn("SELECT * FROM on_timezones")
            self.assertEqual(len(rows), 3)
            _ = [dict(zip(cols, i)) for i in rows]
            for i in _:
                _ = i['ts_with_tz'].astimezone(timezone(i['tz_at_insert']))
                self.assertEqual(i['ts_without_tz'], _.replace(tzinfo=None))
        with withdb.factory(self.cfg) as conn:
            conn("DROP TABLE IF EXISTS on_timezones")


if __name__ == '__main__':
    main()
