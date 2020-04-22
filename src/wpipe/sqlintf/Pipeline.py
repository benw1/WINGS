from .core import *


class Pipeline(Base):
    __tablename__ = 'pipelines'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(256))
    timestamp = sa.Column(sa.TIMESTAMP)
    pipe_root = sa.Column(sa.String(256))
    software_root = sa.Column(sa.String(256))
    input_root = sa.Column(sa.String(256))
    data_root = sa.Column(sa.String(256))
    config_root = sa.Column(sa.String(256))
    description = sa.Column(sa.String(256))
    user_id = sa.Column(sa.Integer, sa.ForeignKey('users.id'))
    user = orm.relationship("User", back_populates="pipelines")
    inputs = orm.relationship("Input", back_populates="pipeline")
    tasks = orm.relationship("Task", back_populates="pipeline")
