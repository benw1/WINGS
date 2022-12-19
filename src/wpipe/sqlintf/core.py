#!/usr/bin/env python
"""
Contains the core import statements and developing tools of sqlintf

Please note that this module is private. All functions and objects
are available in the main ``wpipe.sqlintf`` namespace - use that instead.
"""
import os
import urllib.parse
import typing
import contextlib
import argparse
from pathlib import Path
import tenacity as tn
import sqlalchemy as sa
from sqlalchemy import orm, exc, pool
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['contextlib','argparse', 'tn', 'sa', 'orm', 'exc', 'pool', 'PARSER',
           'verbose', 'Engine', 'Base', 'Session']

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
# my_file = Path("/usr/lusers/benw1/server.address")
if sqlite:
    ENGINE_URL = 'sqlite:///:memory:'
elif 'WPIPE_ENGINEURL' in os.environ.keys():
    ENGINE_URL = os.environ['WPIPE_ENGINEURL']
elif PARSER.parse_known_args()[0].test:
    ENGINE_URL = "mysql+pymysql://root:password@localhost:8000/server"
# elif my_file.is_file():
#     ip1 = my_file.read_text()
#     ip = ip1.strip()
#     ENGINE_URL = 'mysql://wings:wings2025@'+ip+':8020/server'
else:
    raise ImportError("You must provide an engine URL via the environment variable WPIPE_ENGINEURL")
#else:
#    ENGINE_URL = 'mysql://wings:wings2025@10.64.57.84:8020/server'
#    # ENGINE_URL = 'mysql+mysqlconnector://root:password@localhost:8000/server'


POOL_RECYLE = 3600

def make_engine():
    url_parse_results = urllib.parse.urlparse(ENGINE_URL)
    if Path(url_parse_results.hostname).is_file():
        ip = Path(url_parse_results.hostname).read_text().strip()
        engine_url = url_parse_results._replace(
                            netloc=url_parse_results.netloc.replace(
                                f"@{url_parse_results.hostname}:", f"@{ip}:")).get_url()
    else:
        engine_url = ENGINE_URL
    return sa.create_engine(engine_url, echo=verbose, pool_recycle=POOL_RECYLE)

Engine = make_engine()
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
    Engine = make_engine()

Base = declarative_base()
"""
sqlalchemy.ext.declarative.api.Base class: Base class to sqlintf classes.
"""


def _base_get_id(self):
    return self._sa_instance_state.key[1][0]


setattr(Base, 'get_id', _base_get_id)

# adapted from https://stackoverflow.com/a/55749579
def _base___repr__(self) -> str:
        kwargs = {key: getattr(self, key) for key in ['id']+self.__UNIQ_ATTRS__}
        return self._repr(**kwargs)

def _base__repr(self, **fields: typing.Dict[str, typing.Any]) -> str:
    '''
    Helper for __repr__
    '''
    field_strings = []
    at_least_one_attached_attribute = False
    for key, field in fields.items():
        try:
            field_strings.append(f'{key}={field!r}')
        except sa.orm.exc.DetachedInstanceError:
            field_strings.append(f'{key}=DetachedInstanceError')
        else:
            at_least_one_attached_attribute = True
    if at_least_one_attached_attribute:
        return f"<{self.__class__.__name__}({', '.join(field_strings)})>"
    return f"<{self.__class__.__name__} {id(self)}>"

setattr(Base, '__UNIQ_ATTRS__', [])
setattr(Base, '__repr__', _base___repr__)
setattr(Base, '_repr', _base__repr)

Session = orm.sessionmaker(bind=Engine)
"""
sqlalchemy.orm.session.Session class: initiates new sessions bound to the engine. 
"""
