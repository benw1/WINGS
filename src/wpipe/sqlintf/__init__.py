#!/usr/bin/env python
"""
sqlintf
=====
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
"""
from .core import argparse, sa, orm, PARSER, session, Base
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

__all__ = ['sa', 'orm', 'argparse', 'PARSER', 'session', 'Base', 'User',
           'Node', 'Pipeline', 'DPOwner', 'Input', 'Option', 'OptOwner',
           'Target', 'Configuration', 'Parameter', 'DataProduct', 'Task',
           'Mask', 'Job', 'Event']

Base.metadata.create_all(session.bind)

# import eralchemy as ERA
# ERA.render_er(wp.si.Base,"UML.pdf")
