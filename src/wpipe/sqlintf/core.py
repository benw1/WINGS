import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

engine_URL = 'sqlite:///:memory:'
engine = sa.create_engine(engine_URL, echo=True)
Base = declarative_base()

Session = orm.sessionmaker(bind=engine)
session = Session()
