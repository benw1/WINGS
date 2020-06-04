#!/usr/bin/env python
"""
Contains the sqlintf.Parameter class definition

Please note that this module is private. The sqlintf.Parameter class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['Parameter']


class Parameter(Base):
    """
        A Parameter object represents a row of the `parameters` table.

        DO NOT USE CONSTRUCTOR: constructing a Parameter object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'parameters'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="parameters")
