#!/usr/bin/env python
# -*- coding: utf-8; mode: python; -*-

"""Test drive all base/basic features of the module"""

from io import StringIO
from logging import Formatter, StreamHandler, getLevelName, getLogger

FMT = '%(asctime)s %(name)s %(filename)s:%(lineno)d '
FMT += '%(levelname)s: %(funcName)s: %(message)s'


def logger_output_for(logger_name, logging_level):
    """Returns a StringIO that holds what logger_name will log for a given
    logging level"""
    logger = getLogger(logger_name)
    logger.setLevel(getLevelName(logging_level))
    iostr = StringIO()
    sh = StreamHandler(iostr)
    sh.setLevel(getLevelName(logging_level))
    sh.setFormatter(Formatter(FMT))
    logger.addHandler(sh)
    return iostr
