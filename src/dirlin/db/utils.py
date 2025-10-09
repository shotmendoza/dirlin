from copy import copy
from pathlib import Path

import sqlalchemy as sa

from sqlalchemy import make_url
from sqlalchemy.exc import ProgrammingError, OperationalError


# taken from sqlalchemy_utils

def _sqlite_file_exists(database):
    """confirm whether the database path exists and is a file, or
    if the path represents a size greater than 100.

    We'll then confirm the header format.

    """
    if not Path(database).is_file() or Path(database).stat().st_size < 100:
        return False

    with open(database, 'rb') as f:
        header = f.read(100)

    return header[:16] == b'SQLite format 3\x00'


def _get_scalar_result(engine, sql):
    with engine.connect() as conn:
        return conn.scalar(sql)


def _set_url_database(url: sa.engine.url.URL, database):
    """Set the database of an engine URL.

    :param url: A SQLAlchemy engine URL.
    :param database: New database to set.

    """
    if hasattr(url, '_replace'):
        # Cannot use URL.set() as database may need to be set to None.
        ret = url._replace(database=database)
    else:  # SQLAlchemy <1.4
        url = copy(url)
        url.database = database
        ret = url
    assert ret.database == database, ret
    return ret


def database_exists(url):
    """Check if a database exists.

    :param url: A SQLAlchemy engine URL.

    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::

        database_exists('postgresql://postgres@localhost/name')  #=> False
        create_database('postgresql://postgres@localhost/name')
        database_exists('postgresql://postgres@localhost/name')  #=> True

    Supports checking against a constructed URL as well. ::

        engine = create_engine('postgresql://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    engine = None
    try:
        if dialect_name == 'postgresql':
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, 'postgres', 'template1', 'template0', None):
                url = _set_url_database(url, database=db)
                engine = sa.create_engine(url)
                try:
                    return bool(_get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == 'mysql':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            text = ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = '%s'" % database)
            return bool(_get_scalar_result(engine, sa.text(text)))

        elif dialect_name == 'sqlite':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            if database:
                return database == ':memory:' or _sqlite_file_exists(database)
            else:
                return True
        else:
            text = 'SELECT 1'
            try:
                engine = sa.create_engine(url)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except (ProgrammingError, OperationalError):
                return False
    finally:
        if engine:
            engine.dispose()
