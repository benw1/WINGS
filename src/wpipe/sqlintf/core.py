import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

engine_URL = 'sqlite:///:memory:'
#engine_URL = 'mysql://wpipe:W£|£3u53r@localhost/wpipe'
engine = sa.create_engine(engine_URL)#, echo=True)
Base = declarative_base()

Session = orm.sessionmaker(bind=engine)
session = Session()
