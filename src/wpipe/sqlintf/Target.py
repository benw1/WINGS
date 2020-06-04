#!/usr/bin/env python
"""
Contains the sqlintf.Target class definition

Please note that this module is private. The sqlintf.Target class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .OptOwner import OptOwner

__all__ = ['Target']


class Target(OptOwner):
    """
        A Target object represents a row of the `targets` table.

        DO NOT USE CONSTRUCTOR: constructing a Target object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'targets'
    id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    datapath = sa.Column(sa.String(256))
    dataraws = sa.Column(sa.String(256))
    input_id = sa.Column(sa.Integer, sa.ForeignKey('inputs.id'))
    input = orm.relationship("Input", back_populates="targets")
    configurations = orm.relationship("Configuration", back_populates="target")
    __mapper_args__ = {
        'polymorphic_identity': 'target',
    }
