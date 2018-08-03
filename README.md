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
