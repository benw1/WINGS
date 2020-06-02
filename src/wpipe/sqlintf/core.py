#!/usr/bin/env python
"""
Contains the core import statements and developing tools of sqlintf

Please note that this module is private. All functions and objects
are available in the main ``sqlintf`` namespace - use that instead.
"""
import argparse

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

PARSER = argparse.ArgumentParser()
"""
argparse.ArgumentParser object: pre-instantiated parser.

For usage, call object-method PARSER.print_help in interpreter.
"""
PARSER.add_argument('--sqlite', '-s', dest='sqlite', action='store_true',
                    help='Use the in-memory sql database for testing purpose')

sqlite = PARSER.parse_known_args()[0].sqlite
"""
boolean: flag to call with the parser to use the in-memory sql database.
"""

if sqlite:
    ENGINE_URL = 'sqlite:///:memory:'
    #
    # def open_interpreter():
    #     import code
    #     code.interact(local=dict(globals(), **locals()))
    #
    # import atexit
    #
    # atexit.register(open_interpreter)
else:
    ENGINE_URL = 'mysql://wpipe:W£|£3u53r@localhost/server'

engine = sa.create_engine(ENGINE_URL)  # , echo=True)
"""
sqlalchemy.engine.base.Engine object: handles the connection to the database.
"""

if not sqlite:
    engine.execute("CREATE DATABASE IF NOT EXISTS wpipe")
    engine.execute("USE wpipe")

Base = declarative_base()
"""
sqlalchemy.ext.declarative.api.DeclarativeMeta: Base class to sqlintf classes.
"""

session = orm.sessionmaker(bind=engine)()
"""
sqlalchemy.orm.session.Session: manages database operations via the engine.
"""
