#!/usr/bin/env python
"""
Contains the sqlintf.OptOwner class definition

Please note that this module is private. The sqlintf.OptOwner class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['OptOwner']


class OptOwner(Base):
    """
        A OptOwner object represents a row of the `optowners` table.

        DO NOT USE CONSTRUCTOR: constructing a OptOwner object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'optowners'
    id = sa.Column(sa.Integer, primary_key=True)
    timestamp = sa.Column(sa.TIMESTAMP)
    type = sa.Column(sa.String(256))
    options = orm.relationship("Option", back_populates="optowner")
    __mapper_args__ = {
        'polymorphic_identity': 'optowner',
        'polymorphic_on': type
    }

