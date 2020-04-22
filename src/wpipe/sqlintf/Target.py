from .core import *
from .OptOwner import OptOwner


class Target(OptOwner):
    __tablename__ = 'targets'
    id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'), primary_key=True)
    name = sa.Column(sa.String(256))
    datapath = sa.Column(sa.String(256))
    dataraws = sa.Column(sa.String(256))
    input_id = sa.Column(sa.Integer, sa.ForeignKey('inputs.id'))
    input = orm.relationship("Input", back_populates="targets")
    configurations = orm.relationship("Configuration", back_populates="target")
    __mapper_args__ = {
        'polymorphic_identity': 'target',
    }
