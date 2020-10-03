#!/usr/bin/env python
"""
Contains the core import statements and developing tools of sqlintf

Please note that this module is private. All functions and objects
are available in the main ``sqlintf`` namespace - use that instead.
"""
import os
import argparse

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy.exc as exc
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['argparse', 'sa', 'orm', 'exc', 'PARSER', 'engine', 'Base', 'session']

PARSER = argparse.ArgumentParser()
"""
argparse.ArgumentParser object: pre-instantiated parser.

For usage, call object-method PARSER.print_help in interpreter.
"""
PARSER.add_argument('--sqlite', '-s', dest='sqlite', action='store_true',
                    help='Use the in-memory sql database for testing purpose')
PARSER.add_argument('--github-test', dest='test', action='store_true',
                    help='Used when performing continuous integration on GitHub')

sqlite = PARSER.parse_known_args()[0].sqlite
"""
boolean: flag to call with the parser to use the in-memory sql database.
"""

if sqlite:
    ENGINE_URL = 'sqlite:///:memory:'
elif 'WPIPE_ENGINEURL' in os.environ.keys():
    ENGINE_URL = os.environ['WPIPE_ENGINEURL']
elif PARSER.parse_known_args()[0].test:
    ENGINE_URL = "mysql+pymysql://root:password@localhost:8000/server"
else:
    ENGINE_URL = 'mysql://wings:wings2025@10.150.27.94:8020/server'
    # ENGINE_URL = 'mysql+mysqlconnector://root:password@localhost:8000/server'

engine = sa.create_engine(ENGINE_URL)  # , echo=True)
"""
sqlalchemy.engine.base.Engine object: handles the connection to the database.
"""
# except sa.exc.OperationalError: # revert to some default on error
#     print("We got here")
#     engine = sa.create_engine("mysql+pymysql://root:password@localhost:8000/server")
engine = sa.create_engine("mysql+pymysql://root:password@localhost:8000/server")

if not sqlite:
    # engine.execute("CREATE DATABASE IF NOT EXISTS wpipe")
    engine.execute("USE wpipe")

engine.execute("USE wpipe") # TODO: Make sure to remove

Base = declarative_base()
"""
sqlalchemy.ext.declarative.api.DeclarativeMeta: Base class to sqlintf classes.
"""

session = orm.sessionmaker(bind=engine)()
"""
sqlalchemy.orm.session.Session: manages database operations via the engine.
"""
