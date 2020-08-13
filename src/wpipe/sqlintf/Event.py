#!/usr/bin/env python
"""
Contains the sqlintf.Event class definition

Please note that this module is private. The sqlintf.Event class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm
from .OptOwner import OptOwner

__all__ = ['Event']


class Event(OptOwner):
    """
        A Event object represents a row of the `events` table.

        DO NOT USE CONSTRUCTOR: constructing a Event object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'events'
    id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    tag = sa.Column(sa.String(256))
    jargs = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    parent_job_id = sa.Column(sa.Integer, sa.ForeignKey('jobs.id'))
    parent_job = orm.relationship("Job", back_populates="child_events", foreign_keys=[parent_job_id])
    fired_jobs = orm.relationship("Job", back_populates="firing_event", primaryjoin="Event.id==Job.firing_event_id")
    __mapper_args__ = {
        'polymorphic_identity': 'event',
    }
    __table_args__ = (sa.UniqueConstraint('parent_job_id', 'name', 'tag'),
                      )
