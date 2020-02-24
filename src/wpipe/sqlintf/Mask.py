from .core import *


class Mask(Base):
    __tablename__ = 'masks'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    timestamp = sa.Column(sa.TIMESTAMP)
    source = sa.Column(sa.String)
    value = sa.Column(sa.String)
    task_id = sa.Column(sa.Integer, sa.ForeignKey('tasks.id'))
    task = orm.relationship("Task", back_populates="masks")
