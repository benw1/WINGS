#!/usr/bin/env python
"""
Contains the sqlintf.Job class definition

Please note that this module is private. The sqlintf.Job class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .OptOwner import OptOwner

__all__ = ['Job']


class Job(OptOwner):
    """
        A Job object represents a row of the `jobs` table.

        DO NOT USE CONSTRUCTOR: constructing a Job object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'jobs'
    id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'), primary_key=True)
    state = sa.Column(sa.String(256))
    starttime = sa.Column(sa.TIMESTAMP)
    endtime = sa.Column(sa.TIMESTAMP)
    node_id = sa.Column(sa.Integer, sa.ForeignKey('nodes.id'))
    node = orm.relationship("Node", back_populates="jobs")
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="jobs")
    task_id = sa.Column(sa.Integer, sa.ForeignKey('tasks.id'))
    task = orm.relationship("Task", back_populates="jobs")
    firing_event_id = sa.Column(sa.Integer, sa.ForeignKey('events.id'))
    firing_event = orm.relationship("Event", primaryjoin="Job.firing_event_id==Event.id", back_populates="fired_jobs")
    child_events = orm.relationship("Event", primaryjoin="Job.id==Event.parent_job_id", back_populates="parent_job")
    __mapper_args__ = {
        'polymorphic_identity': 'job',
    }
