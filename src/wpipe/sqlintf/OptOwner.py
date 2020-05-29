from .core import *


class OptOwner(Base):
    __tablename__ = 'optowners'
    id = sa.Column(sa.Integer, primary_key=True)
    timestamp = sa.Column(sa.TIMESTAMP)
    type = sa.Column(sa.String(256))
    options = orm.relationship("Option", back_populates="optowner")
    __mapper_args__ = {
        'polymorphic_identity': 'optowner',
        'polymorphic_on': type
    }

