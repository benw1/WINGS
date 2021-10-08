#!/usr/bin/env python
"""
Contains the proxies.BaseProxy class definition for all inherited proxy
classes

Please note that this module is private. The proxies.BaseProxy class is
available in the ``wpipe.proxies`` namespace - use that instead.
"""
from .core import numbers, datetime, si, in_session, try_scalar

__all__ = ['BaseProxy']


class BaseProxy:
    """
        Parent class of all proxy classes

        Parameters
        ----------
        parents : sqlintf.Base object
            TODO
        attr_name : string
            TODO
        try_scalar : boolean
            TODO

        Attributes
        ----------
        parents : sqlintf.Base object
            TODO
        parent_id : int
            TODO
        attr_name : string
            TODO
        try_scalar : boolean
            TODO
    """
    def __new__(cls, *args, **kwargs):
        if cls is BaseProxy:
            parent = kwargs.pop('parent', None)
            with si.begin_session() as session:
                session.add(parent)
                proxy = getattr(parent, kwargs.pop('attr_name', ''))
            if kwargs.pop('try_scalar', False):
                proxy = try_scalar(proxy)
            if proxy is None:
                cls = type(None)
                args = []
                kwargs = {}
            else:
                if isinstance(proxy, str) or isinstance(proxy, numbers.Number):
                    from . import StrNumProxy
                    cls = StrNumProxy
                elif isinstance(proxy, datetime.datetime):
                    from . import DatetimeProxy
                    cls = DatetimeProxy
                else:
                    raise ValueError("Invalid proxy type %s" % type(proxy))
                args = [proxy]
            return cls.__new__(cls, *args, *kwargs)
        return super().__new__(cls, *args, *kwargs)

    def __init__(self, *args, **kwargs):
        self._parent = kwargs.pop('parent', None)
        self._attr_name = kwargs.pop('attr_name', '')
        self._try_scalar = kwargs.pop('try_scalar', False)
        self._session = None
        self._parent_id = self._get_parent_id()

    @property
    def parent(self):
        """
        TODO
        """
        return self._parent

    @property
    def parent_id(self):
        """
        TODO
        """
        return self._parent_id

    @property
    def attr_name(self):
        """
        TODO
        """
        return self._attr_name

    @property
    def try_scalar(self):
        """
        TODO
        """
        return self._try_scalar

    @in_session('parent')
    def delete(self):
        si.delete(self._parent)

    @in_session('parent')
    def _get_parent_id(self):
        return int(self._parent.id)

    @in_session('parent')
    def _augmented_assign(self, operator, other):
        """
        TODO
        """
        for retry in self._session.retrying_nested():
            with retry:
                _temp = retry.retry_state.query(self.parent.__class__).with_for_update(). \
                    filter_by(id=self.parent_id).one()
                retry.retry_state.refresh(_temp)
                _result = getattr(
                    [lambda x: x, try_scalar][self._try_scalar](getattr(_temp, self.attr_name)),
                    operator)(other)
                if _result is not NotImplemented:
                    setattr(_temp, self.attr_name, _result)
                    _temp = BaseProxy(parent=self.parent,
                                      attr_name=self.attr_name,
                                      try_scalar=self.try_scalar)
                retry.retry_state.commit()
        if _result is NotImplemented:
            raise TypeError("unsupported operand type(s) for augmented assignment")
        else:
            return _temp
