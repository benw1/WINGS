#!/usr/bin/env python
"""
Contains the sqlintf.Input class definition

Please note that this module is private. The sqlintf.Input class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .DPOwner import DPOwner

__all__ = ['Input']


class Input(DPOwner):
    """
        A Input object represents a row of the `inputs` table.

        DO NOT USE CONSTRUCTOR: constructing a Input object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'inputs'
    id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    rawspath = sa.Column(sa.String(256))
    confpath = sa.Column(sa.String(256))
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="inputs")
    targets = orm.relationship("Target", back_populates="input")
    __mapper_args__ = {
        'polymorphic_identity': 'input',
    }

