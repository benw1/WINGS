#!/usr/bin/env python
"""
Contains the core import statements and developing tools of sqlintf

Please note that this module is private. All functions and objects
are available in the main ``wpipe.sqlintf`` namespace - use that instead.
"""
import os
import argparse

import tenacity as tn
import sqlalchemy as sa
from sqlalchemy import orm, exc, pool
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['argparse', 'tn', 'sa', 'orm', 'exc', 'pool', 'PARSER', 'verbose',
           'Engine', 'Base', 'Session']

PARSER = argparse.ArgumentParser()
"""
argparse.ArgumentParser object: pre-instantiated parser.

For usage, call object-method PARSER.print_help in interpreter.
"""
PARSER.add_argument('--verbose', '-v', dest='verbose', action='store_true',
                    help='Verbose mode for wpipe')
PARSER.add_argument('--sqlite', '-s', dest='sqlite', action='store_true',
                    help='Use the in-memory sql database for testing purpose')
PARSER.add_argument('--github-test', dest='test', action='store_true',
                    help='Used when performing continuous integration on GitHub')

sqlite = PARSER.parse_known_args()[0].sqlite
"""
boolean: flag to call with the parser to use the in-memory sql database.
"""

verbose = PARSER.parse_known_args()[0].verbose

if sqlite:
    ENGINE_URL = 'sqlite:///:memory:'
elif 'WPIPE_ENGINEURL' in os.environ.keys():
    ENGINE_URL = os.environ['WPIPE_ENGINEURL']
elif PARSER.parse_known_args()[0].test:
    ENGINE_URL = "mysql+pymysql://root:password@localhost:8000/server"
else:
    ENGINE_URL = 'mysql://wings:wings2025@10.150.27.94:8020/server'
    # ENGINE_URL = 'mysql+mysqlconnector://root:password@localhost:8000/server'

POOL_RECYLE = 3600

Engine = sa.create_engine(ENGINE_URL, echo=verbose, pool_recycle=POOL_RECYLE)
"""
sqlalchemy.engine.base.Engine object: handles the connection to the database.
"""
# except sa.exc.OperationalError: # revert to some default on error
#     print("We got here")
#     engine = sa.create_engine("mysql+pymysql://root:password@localhost:8000/server")
# engine = sa.create_engine("mysql+pymysql://root:password@localhost:8000/server")  # This is for the mysql container

if not sqlite:
    Engine.execute("CREATE DATABASE IF NOT EXISTS wpipe")
    Engine.execute("USE wpipe")
    ENGINE_URL = ENGINE_URL.replace('server', 'wpipe')
    Engine.dispose()
    Engine = sa.create_engine(ENGINE_URL, echo=verbose, pool_recycle=POOL_RECYLE)

Base = declarative_base()
"""
sqlalchemy.ext.declarative.api.Base class: Base class to sqlintf classes.
"""

Session = orm.sessionmaker(bind=Engine)
"""
sqlalchemy.orm.session.Session class: initiates new sessions bound to the engine. 
"""
