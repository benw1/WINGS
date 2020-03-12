from .core import *
from .Owner import Owner


class Event(Owner):
    __tablename__ = 'events'
    id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    jargs = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    parent_job_id = sa.Column(sa.Integer, sa.ForeignKey('jobs.id'))
    parent_job = orm.relationship("Job", primaryjoin="Event.parent_job_id==Job.id", back_populates="child_events")
    fired_jobs = orm.relationship("Job", primaryjoin="Event.id==Job.firing_event_id", back_populates="firing_event")
    __mapper_args__ = {
        'polymorphic_identity': 'event',
    }
