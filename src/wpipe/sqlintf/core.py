import argparse
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--sqlite', '-s', dest='sqlite', action='store_true',
                    help='Use the in-memory sql database for testing purpose')
sqlite = PARSER.parse_known_args()[0].sqlite

if sqlite:
    engine_URL = 'sqlite:///:memory:'
    #
    # def open_interpreter():
    #     import code
    #     code.interact(local=dict(globals(), **locals()))
    #
    # import atexit
    #
    # atexit.register(open_interpreter)
else:
    engine_URL = 'mysql+mysqlconnector://root:password@localhost:8000/server'

engine = sa.create_engine(engine_URL)  # , echo=True)

if not sqlite:
    engine.execute("CREATE DATABASE IF NOT EXISTS wpipe")
    engine.execute("USE wpipe")

Base = declarative_base()

Session = orm.sessionmaker(bind=engine)
session = Session()
