#!/usr/bin/env python
"""
Contains the sqlintf.Pipeline class definition

Please note that this module is private. The sqlintf.Pipeline class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .DPOwner import DPOwner

__all__ = ['Pipeline']


class Pipeline(DPOwner):
    """
        A Pipeline object represents a row of the `pipelines` table.

        DO NOT USE CONSTRUCTOR: constructing a Pipeline object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __UNIQ_ATTRS__ = ['user_id', 'pipe_root']
    __tablename__ = 'pipelines'
    id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    pipe_root = sa.Column(sa.String(256))
    software_root = sa.Column(sa.String(256))
    input_root = sa.Column(sa.String(256))
    data_root = sa.Column(sa.String(256))
    config_root = sa.Column(sa.String(256))
    description = sa.Column(sa.String(256))
    user_id = sa.Column(sa.Integer, sa.ForeignKey('users.id'))
    user = orm.relationship("User", back_populates="pipelines")
    inputs = orm.relationship("Input", back_populates="pipeline", primaryjoin="Pipeline.id==Input.pipeline_id")
    tasks = orm.relationship("Task", back_populates="pipeline")
    __mapper_args__ = {
        'polymorphic_identity': 'pipeline',
    }
    __table_args__ = (sa.UniqueConstraint('user_id', 'pipe_root'),
                      )
