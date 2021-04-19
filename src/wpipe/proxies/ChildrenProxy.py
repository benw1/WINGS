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
        self._hold_struct_children = None
        self._session = None
        self._parent_id = self._get_parent_id()

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

    def __getitem__(self, item):
        if isinstance(item, str):
            child = self._search_child_from_attritem(item)
            if child is None:
                raise KeyError(item)
            else:
                return child
        elif np.ndim(item) == 0:
            return self._get_child_of_index(item)
        elif hasattr(item, '__len__'):  # TODO: what about slices?
            return [self[i] for i in np.arange(len(self))[item]]  # TODO: cannot work well if collection change size
        else:
            raise TypeError  # TODO

    @in_session('_parent')
    def __getattr__(self, item):
        if hasattr(getattr(sys.modules['wpipe'], self._cls_name), item):
            self._refresh()
            with self._with_sqlintf():
                return np.array([getattr(self[i], item) for i in range(len(self))])

    @property
    def children(self):
        if self._hold_struct_children is None:
            return getattr(self._parent, self._children_attr)
        else:
            return self._hold_struct_children

    @in_session('_parent')
    def delete(self):
        while len(self):
            self[0].delete()

    @contextlib.contextmanager
    @in_session('_parent', generator=True)
    def hold_structure(self):
        self._hold_struct_children = list(self.children)
        try:
            yield
        finally:
            self._hold_struct_children = None

    @in_session('_parent')
    def _get_parent_id(self):
        return int(self._parent.id)

    @in_session('_parent')
    def _get_child_of_index(self, item):
        if self._work_with_sqlintf == 0:
            return getattr(sys.modules['wpipe'], self._cls_name)(self.children[item])
        else:
            return self.children[item]

    @in_session('_parent')
    def _search_child_from_attritem(self, item):
        for retry in self._session.retrying_nested():
            with retry:
                _temp = retry.retry_state.query(self._parent.__class__).with_for_update(). \
                    filter_by(id=self._parent_id).one()
                _temp = None
                for child in self.children:
                    if getattr(child, self._child_attr) == item:
                        _temp = child
                        break
                retry.retry_state.commit()
        if _temp is not None:
            if self._work_with_sqlintf == 0:
                return getattr(sys.modules['wpipe'], self._cls_name)(_temp)
            else:
                return _temp

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
