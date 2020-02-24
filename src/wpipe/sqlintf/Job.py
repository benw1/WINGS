from .core import *
from .Owner import Owner


class Job(Owner):
    __tablename__ = 'jobs'
    id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'), primary_key=True)
    state = sa.Column(sa.String)
    starttime = sa.Column(sa.TIMESTAMP)
    endtime = sa.Column(sa.TIMESTAMP)
    node_id = sa.Column(sa.Integer, sa.ForeignKey('nodes.id'))
    node = orm.relationship("Node", back_populates="jobs")
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="jobs")
    task_id = sa.Column(sa.Integer, sa.ForeignKey('tasks.id'))
    task = orm.relationship("Task", back_populates="jobs")
    event = orm.relationship("Event", uselist=False, primaryjoin="Job.id==Event.job_id", back_populates="job")
    __mapper_args__ = {
        'polymorphic_identity':'job',
    }
