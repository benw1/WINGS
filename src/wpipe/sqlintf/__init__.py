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

SESSION
    session which SQLAlchemy uses to communicate with the database

COMMIT_FLAG
    boolean flag to control the automatic committing

commit
    Flush and commit pending changes if COMMIT_FLAG is True
"""
from .core import contextlib, argparse, tn, sa, orm, exc, PARSER, Engine, Base, Session
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

__all__ = ['sa', 'orm', 'exc', 'argparse', 'PARSER', 'Session', 'SESSION',
           'User', 'Node', 'Pipeline', 'DPOwner', 'Input', 'Option',
           'OptOwner', 'Target', 'Configuration', 'Parameter', 'DataProduct',
           'Task', 'Mask', 'Job', 'Event',
           'COMMIT_FLAG', 'hold_commit', 'begin_session', 'delete']

Base.metadata.create_all(Engine)

SESSION = None  # Session()  # TODO: SessionPooling
"""
sqlalchemy.orm.session.Session object: placeholder (manages database operations via the engine).
"""

COMMIT_FLAG = True
"""
boolean: flag to control the automatic committing.
"""

INSTANCES = []


def deactivate_commit():
    global COMMIT_FLAG
    COMMIT_FLAG = False


def activate_commit():
    global COMMIT_FLAG
    COMMIT_FLAG = True


class HoldCommit:
    def __init__(self):
        self.existing_flag = COMMIT_FLAG

    def __enter__(self):
        if self.existing_flag:
            deactivate_commit()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.existing_flag:
            activate_commit()


def hold_commit():
    return HoldCommit()


class BeginSession:
    def __init__(self, **local_kw):
        global SESSION, INSTANCES
        self.EXISTING_SESSION = SESSION is not None
        if self.EXISTING_SESSION:
            self.SESSION = SESSION
        else:
            SESSION = self.SESSION = Session(**local_kw)
            self.add_all(INSTANCES)

    def __dir__(self):
        return super(BeginSession, self).__dir__() + self.SESSION.__dir__()

    def __enter__(self):
        if not self.EXISTING_SESSION and self.SESSION.autocommit:
            self.begin()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        global SESSION, INSTANCES
        if not self.EXISTING_SESSION:
            INSTANCES = self.identity_map.values()[:]
            self.close()
            SESSION = None
            del self.SESSION

    def __getattr__(self, item):
        if item in self.SESSION.__dir__():
            return getattr(self.SESSION, item)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, item))

    def commit(self):
        if COMMIT_FLAG:
            self.SESSION.commit()

    def retrying_nested(self):

        def before(retry_state):
            # TODO: Note for myself: retry_state attrs carry over subsequent attempts!
            if not hasattr(retry_state, 'session'):
                retry_state.session = self
                retry_state.begin_nested = retry_state.session.begin_nested
                retry_state.query = retry_state.session.query
                retry_state.refresh = retry_state.session.refresh
                retry_state.TRANSACTION = retry_state.begin_nested()

                @tn.retry(retry=tn.retry_if_exception_type(exc.OperationalError))
                def _rollback():
                    if retry_state.TRANSACTION.is_active:
                        retry_state.TRANSACTION.rollback()
                    retry_state.TRANSACTION = retry_state.begin_nested()

                retry_state.rollback = _rollback

                def _commit():
                    if retry_state.TRANSACTION.is_active:
                        retry_state.TRANSACTION.commit()
                    if COMMIT_FLAG:
                        retry_state.session.commit()

                retry_state.commit = _commit

        def after(retry_state):
            try:
                retry_state.outcome.result()
            except (exc.OperationalError, exc.IntegrityError) as Err:
                print("Encountered %s\n%s\n\nAttempting rollback\n" % (Err.orig, Err.statement))
            retry_state.rollback()

        return tn.Retrying(retry=(tn.retry_if_exception_type(exc.OperationalError) |
                                  tn.retry_if_exception_type(exc.IntegrityError)),
                           wait=tn.wait_random_exponential(multiplier=0.1),
                           before=before,
                           after=after)


@contextlib.contextmanager
def retrying_session(retry, session):
    with retry, session:
        yield session


def begin_session(**local_kw):
    for retry in tn.Retrying(retry=tn.retry_if_exception_type(exc.OperationalError),
                             after=lambda retry_state:
                             print("Failed attempt to access database; entering retrying loop")
                             if retry_state.attempt_number == 1 else None,
                             wait=tn.wait_random()):
        retry.session = BeginSession(**local_kw)
        yield retrying_session(retry, retry.session)


# def begin_session(**local_kw):
#     for retry in tn.Retrying(retry=tn.retry_if_exception_type(exc.OperationalError)):
#         retry.session = BeginSession(**local_kw)
#         with retry:
#             with retry.session:
#                 yield retry.session


# def begin_session(**local_kw):
#     return BeginSession(**local_kw)


def delete(entry):
    """
    Delete entry from the database.

    Parameters
    ----------
    entry : sqlintf object
        Proxy of entry to delete.
    """
    for session in begin_session():
        with session as session:
            session.delete(entry)
            session.commit()


def show_engine_status():
    for session in begin_session():
        with session as session:
            a = session.execute("SHOW ENGINE INNODB STATUS;").fetchall()
    return a[0][2]


def show_transactions_status():
    a = show_engine_status()
    return a.split('\nTRANSACTIONS\n')[1].split('\nFILE I/O\n')[0]

# import eralchemy as ERA
# ERA.render_er(wp.si.Base,"UML.pdf")
