from .core import *
from .Owner import Owner


class DataProduct(Owner):
    __tablename__ = 'dataproducts'
    id = sa.Column(sa.Integer, sa.ForeignKey('owners.id'), primary_key=True)
    filename = sa.Column(sa.String)
    relativepath = sa.Column(sa.String)
    suffix = sa.Column(sa.String)
    data_type = sa.Column(sa.String)
    subtype = sa.Column(sa.String)
    group = sa.Column(sa.String)
    filtername = sa.Column(sa.String)
    ra = sa.Column(sa.Float)
    dec = sa.Column(sa.Float)
    pointing_angle = sa.Column(sa.Float)
    config_id = sa.Column(sa.Integer, sa.ForeignKey('configurations.id'))
    config = orm.relationship("Configuration", back_populates="dataproducts")
    __mapper_args__ = {
        'polymorphic_identity':'dataproduct',
    }
