# withdb

A factory method that creates adaptor classes for databases

Currrently supported databases are MySQL and PostgreSQL but it can be
easily extended to support any other database if a suitable adaptor
class for Python is provided.

## Configuration

You will need a `~/.withdb_test_psql.cfg` file holding the connection
configuration. See sample file `etc/withdb_test_psql_sample.cfg`.

## Examples of usage

   cfg = {
   }

## Logging

There is a logger named `withdb` that is commonly used across the
module and submodules. In order to use it just instantiate it and add
a handler for it:

	from logggin import getLevelName, getLogger
	
	FMT = '%(asctime)s %(name)s %(filename)s:%(lineno)d '
	FMT += '%(levelname)s:%(levelno)s %(funcName)s: %(message)s'

    logger = getLogger('withdb')
	logger.setLevel(getLevelName('DEBUG'))

	sh = StreamHandler()
	sh.setLevel(getLevelName('DEBUG'))
	sh.setFormatter(Formatter(FMT))
	logger.addHandler(sh)

