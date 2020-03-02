from .core import *


class Node(Base):
    __tablename__ = 'nodes'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    int_ip = sa.Column(sa.String(256))
    ext_ip = sa.Column(sa.String(256))
    jobs = orm.relationship("Job", back_populates="node")
