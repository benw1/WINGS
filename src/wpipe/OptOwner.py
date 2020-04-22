from .core import *
from .Option import Option


class OptOwner:
    def __init__(self, options):
        if not hasattr(self, '_optowner'):
            self._optowner = si.OptOwner()
        if not hasattr(self, '_options_proxy'):
            self._options_proxy = DictLikeChildrenProxy(self._optowner, 'options', 'Option')
        self.options = options
        self._optowner.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def optowner_id(self):
        si.session.commit()
        return self._optowner.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._optowner.timestamp

    @property
    def options(self):
        return self._options_proxy

    @options.setter
    def options(self, options):
        for key, value in options.items():
            self.option(name=key, value=value)

    def option(self, *args, **kwargs):
        return Option(self, *args, **kwargs)
