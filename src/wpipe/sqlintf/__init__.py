#!/usr/bin/env python
"""
Description
-----------

Provides the tools to handle the SQL interface with the database.

How to use
----------

This subpackage handles the interface with the SQL database for which the
connection is powered by the third-party module SQLAlchemy. In practice, no
one should ever need to use this subpackage, it contains the tools to
initialize the database connection and query it, as well as duplicates of each
of the Wpipe classes which objects are meant to represent a single row of the
corresponding database table. Instead, these classes are exploited by their
Wpipe counterparts to interact with the database.

Utilities
---------
PARSER
    pre-instantiated parser powered by the module `argparse`

session
    session which SQLAlchemy uses to communicate with the database

COMMIT_FLAG
    boolean flag to control the automatic committing

commit
    Flush and commit pending changes if COMMIT_FLAG is True
"""
from .core import argparse, tn, sa, orm, exc, PARSER, session, Base
from .User import User
from .Node import Node
from .Pipeline import Pipeline
from .DPOwner import DPOwner
from .Input import Input
from .Option import Option
from .OptOwner import OptOwner
from .Target import Target
from .Configuration import Configuration
from .Parameter import Parameter
from .DataProduct import DataProduct
from .Task import Task
from .Mask import Mask
from .Job import Job
from .Event import Event

__all__ = ['sa', 'orm', 'exc', 'argparse', 'PARSER', 'session', 'Base',
           'User', 'Node', 'Pipeline', 'DPOwner', 'Input', 'Option',
           'OptOwner', 'Target', 'Configuration', 'Parameter', 'DataProduct',
           'Task', 'Mask', 'Job', 'Event',
           'COMMIT_FLAG', 'commit', 'rollback', 'begin_nested']

Base.metadata.create_all(session.bind)

COMMIT_FLAG = True
"""
boolean: flag to control the automatic committing.
"""


def commit():
    """
    Flush and commit pending changes if COMMIT_FLAG is True.
    """
    if COMMIT_FLAG:
        session.commit()


rollback = session.rollback

begin_nested = session.begin_nested


def retrying_nested():
    def before(retry_state):
        retry_state.TRANSACTION = begin_nested()

        def _commit():
            try:
                retry_state.TRANSACTION.commit()
            except exc.ResourceClosedError:
                pass
            commit()

        retry_state.commit = _commit

    def after(retry_state):
        try:
            retry_state.outcome.result()
        except exc.OperationalError as Err:
            print("Encountered %s\n%s\n\nAttempting rollback\n" % (Err.orig, Err.statement))
        try:
            retry_state.TRANSACTION.rollback()
            print("Rollback successful\n")
        except exc.OperationalError as Err:
            print("Rollback unsuccessful %s\n%s\n\nProceeding anyway\n" % (Err.orig, Err.statement))

    return tn.Retrying(retry=tn.retry_if_exception_type(exc.OperationalError),
                       wait=tn.wait_random_exponential(multiplier=0.1),
                       before=before,
                       after=after)


def show_engine_status():
    a = session.execute("SHOW ENGINE INNODB STATUS;").fetchall()
    return a[0][2]


def show_transactions_status():
    a = show_engine_status()
    return a.split('\nTRANSACTIONS\n')[1].split('\nFILE I/O\n')[0]

# import eralchemy as ERA
# ERA.render_er(wp.si.Base,"UML.pdf")
