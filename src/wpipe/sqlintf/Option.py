from .core import *


class Option(Base):
    __tablename__ = 'options'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    value = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    optowner_id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'))
    optowner = orm.relationship("OptOwner", back_populates="options")
