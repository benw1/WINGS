#!/usr/bin/env python
"""
Contains the sqlintf.Node class definition

Please note that this module is private. The sqlintf.Node class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['Node']


class Node(Base):
    """
        A Node object represents a row of the `nodes` table.

        DO NOT USE CONSTRUCTOR: constructing a Node object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __UNIQ_ATTRS__ = ['name']
    __tablename__ = 'nodes'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    int_ip = sa.Column(sa.String(256))
    ext_ip = sa.Column(sa.String(256))
    jobs = orm.relationship("Job", back_populates="node")
    __table_args__ = (sa.UniqueConstraint('name'),
                      )
