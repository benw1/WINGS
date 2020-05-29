from .core import *
from .DPOwner import DPOwner


class Input(DPOwner):
    __tablename__ = 'inputs'
    id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    rawspath = sa.Column(sa.String(256))
    confpath = sa.Column(sa.String(256))
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="inputs")
    targets = orm.relationship("Target", back_populates="input")
    __mapper_args__ = {
        'polymorphic_identity': 'input',
    }

