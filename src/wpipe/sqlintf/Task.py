from .core import *


class Task(Base):
    __tablename__ = 'tasks'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    nruns = sa.Column(sa.Integer)
    run_time = sa.Column(sa.Float)
    is_exclusive = sa.Column(sa.Boolean)
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="tasks")
    masks = orm.relationship("Mask", back_populates="task")
    jobs = orm.relationship("Job", back_populates="task")
