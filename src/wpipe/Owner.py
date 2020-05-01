from .core import *
from .Option import Option


class Owner:
    def __init__(self, options):
        if not hasattr(self, '_owner'):
            self._owner = si.Owner()
        if not hasattr(self, '_options_proxy'):
            self._options_proxy = DictLikeChildrenProxy(self._owner, 'options', 'Option')
        self.options = options
        self._owner.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
        return self._options_proxy

    @options.setter
    def options(self, options):
        for key, value in options.items():
            self.option(name=key, value=value)

    def option(self, *args, **kwargs):
        return Option(self, *args, **kwargs)
