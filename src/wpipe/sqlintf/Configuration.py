from .core import *


class Configuration(Base):
    __tablename__ = 'configurations'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    relativepath = sa.Column(sa.String)
    logpath = sa.Column(sa.String)
    confpath = sa.Column(sa.String)
    rawpath = sa.Column(sa.String)
    procpath = sa.Column(sa.String)
    target_id = sa.Column(sa.Integer, sa.ForeignKey('targets.id'))
    target = orm.relationship("Target", back_populates="configurations")
    dataproducts = orm.relationship("DataProduct", back_populates="config")
    parameters = orm.relationship("Parameter", back_populates="config")
    jobs = orm.relationship("Job", back_populates="config")
