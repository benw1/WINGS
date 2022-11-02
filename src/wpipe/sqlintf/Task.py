#!/usr/bin/env python
"""
Contains the sqlintf.Task class definition

Please note that this module is private. The sqlintf.Task class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['Task']


class Task(Base):
    """
        A Task object represents a row of the `tasks` table.

        DO NOT USE CONSTRUCTOR: constructing a Task object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __UNIQ_ATTRS__ = ['pipeline_id', 'name']
    __tablename__ = 'tasks'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    nruns = sa.Column(sa.Integer)
    run_time = sa.Column(sa.Float)
    is_exclusive = sa.Column(sa.Boolean)
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="tasks")
    masks = orm.relationship("Mask", back_populates="task")
    jobs = orm.relationship("Job", back_populates="task")
    __table_args__ = (sa.UniqueConstraint('pipeline_id', 'name'),
                      )
