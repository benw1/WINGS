from .core import *
from .Node import Node
from .Configuration import Configuration
from .Task import Task


class Job(Base):
    __tablename__ = 'jobs'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    state = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    starttime = sa.Column(sa.TIMESTAMP)
    endtime = sa.Column(sa.TIMESTAMP)
    node_id = sa.Column(sa.Integer, sa.ForeignKey('nodes.id'))
    node = orm.relationship("Node", back_populates="jobs")
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="jobs")
    task_id = sa.Column(sa.Integer, sa.ForeignKey('tasks.id'))
    task = orm.relationship("Task", back_populates="jobs")


Node.jobs = orm.relationship("Job", order_by=Job.id, back_populates="node")
Configuration.jobs = orm.relationship("Job", order_by=Job.id, back_populates="config")
Task.jobs = orm.relationship("Job", order_by=Job.id, back_populates="task")