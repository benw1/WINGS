from .core import *


# Waiting to know more about the utility of the Options class
class Option(Base):
    __tablename__ = 'options'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
