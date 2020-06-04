#!/usr/bin/env python
"""
Contains the sqlintf.User class definition

Please note that this module is private. The sqlintf.User class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['User']


class User(Base):
    """
        A User object represents a row of the `users` table.

        DO NOT USE CONSTRUCTOR: constructing a User object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'users'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    pipelines = orm.relationship("Pipeline", back_populates="user")
