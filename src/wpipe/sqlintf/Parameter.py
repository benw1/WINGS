from .core import *


# Waiting to know more about the utility of the Parameters class
class Parameter(Base):
    __tablename__ = 'parameters'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
