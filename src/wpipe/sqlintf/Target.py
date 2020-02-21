from .core import *
from .Pipeline import Pipeline


class Target(Base):
    __tablename__ = 'targets'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    relativepath = sa.Column(sa.String)
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="targets")


Pipeline.targets = orm.relationship("Target", order_by=Target.id, back_populates="pipeline")
