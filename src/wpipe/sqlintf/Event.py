from .core import *
from .Owner import Owner


class Event(Owner):
    __tablename__ = 'events'
    id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'), primary_key=True)
    name = sa.Column(sa.String)
    jargs = sa.Column(sa.String)
    value = sa.Column(sa.String)
    job_id = sa.Column(sa.Integer, sa.ForeignKey('jobs.id'))
    job = orm.relationship("Job", uselist=False, primaryjoin="Event.job_id==Job.id", back_populates="event")
    __mapper_args__ = {
        'polymorphic_identity':'event',
    }
