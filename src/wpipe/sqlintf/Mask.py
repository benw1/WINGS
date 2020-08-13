#!/usr/bin/env python
"""
Contains the sqlintf.Mask class definition

Please note that this module is private. The sqlintf.Mask class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['Mask']


class Mask(Base):
    """
        A Mask object represents a row of the `masks` table.

        DO NOT USE CONSTRUCTOR: constructing a Mask object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'masks'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    source = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    task_id = sa.Column(sa.Integer, sa.ForeignKey('tasks.id'))
    task = orm.relationship("Task", back_populates="masks")
    __table_args__ = (sa.UniqueConstraint('task_id', 'name'),
                      )
