from .core import *
from .User import User


class Pipeline(Base):
    __tablename__ = 'pipelines'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    software_root = sa.Column(sa.String)
    data_root = sa.Column(sa.String)
    pipe_root = sa.Column(sa.String)
    config_root = sa.Column(sa.String)
    description = sa.Column(sa.String)
    user_id = sa.Column(sa.Integer, sa.ForeignKey('users.id'))
    user = orm.relationship("User", back_populates="pipelines")


User.pipelines = orm.relationship("Pipeline", order_by=Pipeline.id, back_populates="user")
