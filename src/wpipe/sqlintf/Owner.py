from .core import *


class Owner(Base):
    __tablename__ = 'owners'
    id = sa.Column(sa.Integer, primary_key=True)
    timestamp = sa.Column(sa.TIMESTAMP)
    type = sa.Column(sa.String)
    options = orm.relationship("Option", back_populates="owner")
    __mapper_args__ = {
        'polymorphic_identity':'owner',
        'polymorphic_on':type
    }

