from .core import *


class Parameter(Base):
    __tablename__ = 'parameters'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="parameters")
