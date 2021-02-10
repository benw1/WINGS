#!/usr/bin/env python
"""
Contains the proxies.DictLikeChildrenProxy class definition

Please note that this module is private. The proxies.DictLikeChildrenProxy
class is available in the ``wpipe.proxies`` namespace - use that instead.
"""
from .core import sys, itertools, in_session
from .BaseProxy import BaseProxy
from .ChildrenProxy import ChildrenProxy

__all__ = ['DictLikeChildrenProxy']


class DictLikeChildrenProxy(ChildrenProxy):
    """
        Proxy to access children of a Wpipe object in a dictionary format.

        Parameters
        ----------
        parent : sqlintf object
            Parent sqlintf object of Wpipe object associated to this proxy.
        children_attr : string
            Attribute of the sqlintf object returning its children.
        cls_name : string
            Name of the class of the children.
        child_attr : string
            Child attribute that distinguishes the children from one another.
        child_value : string
            Child attribute that returns its stored value.
    """

    def __init__(self, parent, children_attr, cls_name, child_attr='name', child_value='value'):
        super(DictLikeChildrenProxy, self).__init__(parent, children_attr, cls_name, child_attr)
        self._child_value = child_value

    def __repr__(self):
        self._refresh()
        return repr(dict(self._items))

    @in_session('_parent')
    def __getitem__(self, item):
        if isinstance(item, int):
            return super(DictLikeChildrenProxy, self).__getitem__(item)
        self._refresh()
        _temp = self._keys_children
        try:
            key = child = None
            while key != item:
                key, child = next(_temp)
            return BaseProxy(parent=child,
                             attr_name=self._child_value,
                             try_scalar=True)
        except StopIteration:
            raiseerror = True
        if raiseerror:
            raise KeyError(item)

    @in_session('_parent')
    def __setitem__(self, item, value):
        if not isinstance(value, BaseProxy):
            self._refresh()
            _temp = self._keys_children
            try:
                key = None
                count = -1
                while key != item:
                    key, child = next(_temp)
                    count += 1
                child = self.children[count]
                setattr(child, self._child_value, value)
                self._session.commit()
            except StopIteration:
                _temp = getattr(sys.modules['wpipe'], self._cls_name)(
                    getattr(sys.modules['wpipe'], self._parent.__class__.__name__)(self._parent),
                    **{self._child_attr: item, self._child_value: value}
                )

    @property
    def _keys_children(self):
        return map(lambda child: (getattr(child, self._child_attr), child), self.children)

    @property
    def _items(self):
        return itertools.starmap(lambda key, child: (key, getattr(child, self._child_value)), self._keys_children)
