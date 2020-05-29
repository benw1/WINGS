from .core import *
from .DPOwner import DPOwner


class Configuration(DPOwner):
    __tablename__ = 'configurations'
    id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    datapath = sa.Column(sa.String(256))
    confpath = sa.Column(sa.String(256))
    rawpath = sa.Column(sa.String(256))
    logpath = sa.Column(sa.String(256))
    procpath = sa.Column(sa.String(256))
    description = sa.Column(sa.String(256))
    target_id = sa.Column(sa.Integer, sa.ForeignKey('targets.id'))
    target = orm.relationship("Target", back_populates="configurations")
    parameters = orm.relationship("Parameter", back_populates="config")
    jobs = orm.relationship("Job", back_populates="config")
    __mapper_args__ = {
        'polymorphic_identity': 'configuration',
    }
