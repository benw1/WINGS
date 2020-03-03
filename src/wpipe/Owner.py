from .core import *
from .Options import SQLOption


class SQLOwner:
    def __init__(self):
        self._owner = None

    @property
    def owner_id(self):
        si.session.commit()
        return self._owner.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._owner.timestamp

    @property
    def options(self):
        si.session.commit()
        return dict(map(lambda option: [option.name, option.value], self._owner.options))

    @options.setter
    def options(self, options={}):
        for key, value in options.items():
            SQLOption(self, key, value)
