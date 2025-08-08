#!/usr/bin/env python
"""
Contains the sqlintf.Configuration class definition

Please note that this module is private. The sqlintf.Configuration class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .DPOwner import DPOwner

__all__ = ['Configuration']


class Configuration(DPOwner):
    """
        A Configuration object represents a row of the `configurations` table.

        DO NOT USE CONSTRUCTOR: constructing a Configuration object adds a new
        row to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __UNIQ_ATTRS__ = ['target_id', 'name']
    __tablename__ = 'configurations'
    id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    datapath = sa.Column(sa.String(256))
    confpath = sa.Column(sa.String(256))
    rawpath = sa.Column(sa.String(256))
    logpath = sa.Column(sa.String(256))
    procpath = sa.Column(sa.String(256))
    description = sa.Column(sa.String(256))
    target_id = sa.Column(sa.Integer, sa.ForeignKey('targets.id'))
    target = orm.relationship("Target", back_populates="configurations")
    parameters = orm.relationship("Parameter", back_populates="config")
    jobs = orm.relationship("Job", back_populates="config")
    __mapper_args__ = {
        'polymorphic_identity': 'configuration',
    }
    __table_args__ = (sa.UniqueConstraint('target_id', 'name'),
                      )
