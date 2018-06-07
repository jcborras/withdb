# -*- coding: utf-8; mode: python; -*-

from codecs import open
from setuptools import setup

PROJECT_NAME = 'withdb'


def get_property(property):
    with open(PROJECT_NAME + '/__init__.py', 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith(property):
                return eval(line.split('=')[-1])


def get_long_description():
    descr = []
    for i in ['README.md', 'CHANGES.txt', ]:
        with open(i, 'r', encoding='utf-8') as f:
            descr.append(f.read())
    return '\n\n'.join(descr)


setup(name=PROJECT_NAME,
      version=get_property('__version__'),
      description='A factory that produces uniform adaptors for DB operations',
      long_description=get_long_description(),
      url='https://github.com/jcborras/withdb',
      author=get_property('__author__'),
      author_email='jcborras@gmail.com',
      license='MIT License',
      packages=['withdb'],
      install_requires=['mysql-connector-python', 'psycopg2'],
      extras_require={
#          'MySQL': ['mysql-connector-python'],
#          'PostgreSQL': ['psycopg2'],
      },
      scripts=[
          'bin/list_available_adaptors',
      ],
      zip_safe=False)
