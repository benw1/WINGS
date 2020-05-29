from .core import *


class DPOwner(Base):
    __tablename__ = 'dpowners'
    id = sa.Column(sa.Integer, primary_key=True)
    timestamp = sa.Column(sa.TIMESTAMP)
    type = sa.Column(sa.String(256))
    dataproducts = orm.relationship("DataProduct", back_populates="dpowner")
    __mapper_args__ = {
        'polymorphic_identity': 'dpowner',
        'polymorphic_on': type
    }

