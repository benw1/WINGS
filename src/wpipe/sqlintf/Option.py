from .core import *


# Waiting to know more about the utility of the Options class
class Option(Base):
    __tablename__ = 'options'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    owner_id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'))
    owner = orm.relationship("Owner", back_populates="options")
