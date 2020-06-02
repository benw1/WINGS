#!/usr/bin/env python
"""
Contains the sqlintf.Option class definition

Please note that this module is private. The sqlintf.Option class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base


class Option(Base):
    """
        A Option object represents a row of the `options` table.

        DO NOT USE CONSTRUCTOR: constructing a Option object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'options'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    optowner_id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'))
    optowner = orm.relationship("OptOwner", back_populates="options")
