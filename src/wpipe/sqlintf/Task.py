from .core import *
from .Pipeline import Pipeline


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


Pipeline.tasks = orm.relationship("Task", order_by=Task.id, back_populates="pipeline")
