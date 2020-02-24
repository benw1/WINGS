from .core import *
from .Owner import Owner


class Target(Owner):
    __tablename__ = 'targets'
    id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'), primary_key=True)
    name = sa.Column(sa.String)
    relativepath = sa.Column(sa.String)
    pipeline_id = sa.Column(sa.Integer, sa.ForeignKey('pipelines.id'))
    pipeline = orm.relationship("Pipeline", back_populates="targets")
    configurations = orm.relationship("Configuration", back_populates="target")
    __mapper_args__ = {
        'polymorphic_identity':'target',
    }
