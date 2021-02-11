#!/usr/bin/env python
"""
Contains the proxies.ChildrenProxy class definition

Please note that this module is private. The proxies.ChildrenProxy class is
available in the ``wpipe.proxies`` namespace - use that instead.
"""
from .core import contextlib, sys, np, in_session

__all__ = ['ChildrenProxy']


class ChildrenProxy:  # TODO: Generalize proxy object with the BaseProxy
    """
        Proxy to access children of a Wpipe object.

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
    """

    def __init__(self, parent, children_attr, cls_name, child_attr='name'):
        self._parent = parent
        self._children_attr = children_attr
        self._cls_name = cls_name
        self._child_attr = child_attr
        self._work_with_sqlintf = 0
        self._session = None

    def __repr__(self):
        self._refresh()
        return 'Children(' + ', '.join(
            map(lambda child: self._cls_name + '(' + repr(getattr(child, self._child_attr)) + ')',
                self.children)) + ')'

    @in_session('_parent')
    def __len__(self):
        self._refresh()
        return len(self.children)

    def __iter__(self):
        self.n = 0
        self.len = len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        return self

    def __next__(self):
        self.n -= self.len - len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        self.len = len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        if 0 <= self.n < self.len:
            result = self[self.n]
            self.n += 1
            return result
        else:
            del self.n, self.len
            raise StopIteration

    @in_session('_parent')
    def __getitem__(self, item):
        if np.ndim(item) == 0:
            if self._work_with_sqlintf == 0:
                self._refresh()
                if hasattr(self.children[item], '_wpipe_object'):  # TODO: Unnecessary
                    return self.children[item]._wpipe_object
                else:
                    return getattr(sys.modules['wpipe'], self._cls_name)(self.children[item])
            else:
                return self.children[item]
        else:
            return [self[i] for i in np.arange(len(self))[item]]

    @in_session('_parent')
    def __getattr__(self, item):
        if hasattr(getattr(sys.modules['wpipe'], self._cls_name), item):
            self._refresh()
            with self._with_sqlintf():
                return np.array([getattr(self[i], item) for i in range(len(self))])

    @property
    def children(self):
        return getattr(self._parent, self._children_attr)

    @in_session('_parent')
    def delete(self):
        while len(self):
            self[0].delete()

    @contextlib.contextmanager
    def _with_sqlintf(self):
        self._work_with_sqlintf += 1
        try:
            yield
        finally:
            self._work_with_sqlintf -= 1

    @in_session('_parent')
    def _refresh(self, **kwargs):
        if self._work_with_sqlintf == 0:
            self._session.refresh(self._parent, **kwargs)
            for child in self.children:
                self._session.refresh(child, **kwargs)
