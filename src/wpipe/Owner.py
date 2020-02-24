from .Options import SQLOption


class SQLOwner:
    def __init__(self):
        self._owner = None

    @property
    def timestamp(self):
        return self._owner.timestamp

    @property
    def options(self):
        return dict(map(lambda option: [option.name, option.value], self._owner.options))

    @options.setter
    def options(self, options={}):
        for key, value in options.items():
            SQLOption(self._owner, key, value)
