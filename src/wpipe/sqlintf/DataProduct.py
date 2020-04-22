from .core import *
from .OptOwner import OptOwner


class DataProduct(OptOwner):
    __tablename__ = 'dataproducts'
    id = sa.Column(sa.Integer, sa.ForeignKey('optowners.id'), primary_key=True)
    filename = sa.Column(sa.String(256))
    relativepath = sa.Column(sa.String(256))
    suffix = sa.Column(sa.String(256))
    data_type = sa.Column(sa.String(256))
    subtype = sa.Column(sa.String(256))
    group = sa.Column(sa.String(256))
    filtername = sa.Column(sa.String(256))
    ra = sa.Column(sa.Float)
    dec = sa.Column(sa.Float)
    pointing_angle = sa.Column(sa.Float)
    dpowner_id = sa.Column(sa.Integer, sa.ForeignKey('dpowners.id'))
    dpowner = orm.relationship("DPOwner", back_populates="dataproducts")
    config_id = input_id = orm.synonym("dpowner_id")
    config = input = orm.synonym("dpowner")
    __mapper_args__ = {
        'polymorphic_identity': 'dataproduct',
    }
