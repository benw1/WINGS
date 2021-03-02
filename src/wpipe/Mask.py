#!/usr/bin/env python
"""
Contains the Mask class definition

Please note that this module is private. The Mask class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, pd, si
from .core import make_yield_session_if_not_cached, initialize_args, wpipe_to_sqlintf_connection, in_session
from .core import split_path

__all__ = ['Mask']

KEYID_ATTR = 'mask_id'
UNIQ_ATTRS = ['task_id', 'name']
CLASS_LOW = split_path(__file__)[1].lower()


def _in_session(**local_kw):
    return in_session('_%s' % CLASS_LOW, **local_kw)


_check_in_cache = make_yield_session_if_not_cached(KEYID_ATTR, UNIQ_ATTRS, CLASS_LOW)


class Mask:
    """
        Represents a mask associated to a task.

        Call signatures::

            Mask(task, name, source='', value='')
            Mask(keyid)
            Mask(_mask)

        When __new__ is called, it queries the database for an existing
        row in the `masks` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Mask`
        object, otherwise it creates a new row via a new `sqlintf.Mask`
        instance. This `sqlintf.Mask` object is then wrapped under the
        hidden attribute `Mask._mask` in the new instance of this `Mask`
        class generated by __new__.

        All masks are uniquely identified by their task and their name, but
        alternatively, the constructor can take as sole argument either:
         - the primary key id of the corresponding `masks` table row
         - the `sqlintf.Mask` object interfacing that table row

        Parameters
        ----------
        task : Task object
            Parent Task owning this option.
        name : string
            Name of the mask.
        source : string
            Source of the mask - defaults to ''.
        value : string
            Value of the mask - defaults to ''.
        keyid : int
            Primary key id of the table row.
        _mask : sqlintf.Mask object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Task object
            Points to attribute self.task.
        name : string
            Name of the mask.
        mask_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        source : string
            Source of the mask.
        value : string
            Value of the mask.
        task : Task object
            Task object corresponding to parent task.
        task_id : int
            Primary key id of the table row of parent task.
    """
    __cache__ = pd.DataFrame(columns=[KEYID_ATTR]+UNIQ_ATTRS+[CLASS_LOW])

    @classmethod
    def _check_in_cache(cls, kind, loc):
        return _check_in_cache(cls, kind, loc)

    @classmethod
    def _sqlintf_instance_argument(cls):
        if hasattr(cls, '_%s' % CLASS_LOW):
            for _session in cls._check_in_cache(kind='keyid',
                                                loc=getattr(cls, '_%s' % CLASS_LOW)._sa_instance_state.key[1][0]):
                pass

    def __new__(cls, *args, **kwargs):
        if hasattr(cls, '_inst'):
            old_cls_inst = cls._inst
            delattr(cls, '_inst')
        else:
            old_cls_inst = None
        cls._to_cache = {}
        # checking if given argument is sqlintf object or existing id
        cls._mask = args[0] if len(args) else None
        if not isinstance(cls._mask, si.Mask):
            keyid = kwargs.get('id', cls._mask)
            if isinstance(keyid, int):
                for session in cls._check_in_cache(kind='keyid', loc=keyid):
                    cls._mask = session.query(si.Mask).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=3)
                task = kwargs.get('task', wpargs.get('Task', None))
                name = kwargs.get('name', args[0])
                source = kwargs.get('source', '' if args[1] is None else args[1])
                value = kwargs.get('value', '' if args[2] is None else args[2])
                # querying the database for existing row or create
                for session in cls._check_in_cache(kind='args', loc=(task.task_id, name)):
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._mask = this_nested.session.query(si.Mask).with_for_update(). \
                                filter_by(task_id=task.task_id). \
                                filter_by(name=name).one_or_none()
                            if cls._mask is None:
                                cls._mask = si.Mask(name=name,
                                                    source=source,
                                                    value=value)
                                task._task.masks.append(cls._mask)
                                this_nested.commit()
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        else:
            with si.begin_session() as session:
                session.add(cls._mask)
                for _session in cls._check_in_cache(kind='keyid', loc=cls._mask.id):
                    pass
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Mask')
        # add instance to cache dataframe
        if cls._to_cache:
            cls._to_cache[CLASS_LOW] = cls._inst
            cls.__cache__.loc[len(cls.__cache__)] = cls._to_cache
        new_cls_inst = cls._inst
        delattr(cls, '_inst')
        if old_cls_inst is not None:
            cls._inst = old_cls_inst
        return new_cls_inst

    @_in_session()
    def __init__(self, *args, **kwargs):
        self._mask.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Mask objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Mask` for parameters.

        Returns
        -------
        out : list of Mask object
            list of objects fulfilling the kwargs filter.
        """
        with si.begin_session() as session:
            cls._temp = session.query(si.Mask).filter_by(**kwargs)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Task`: Points to attribute self.task.
        """
        return self.task

    @property
    @_in_session()
    def name(self):
        """
        str: Name of the mask.
        """
        self._session.refresh(self._mask)
        return self._mask.name

    @name.setter
    @_in_session()
    def name(self, name):
        self._mask.name = name
        self._mask.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
    def mask_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._mask.id

    @property
    @_in_session()
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        self._session.refresh(self._mask)
        return self._mask.timestamp

    @property
    @_in_session()
    def source(self):
        """
        str: Source of the mask.
        """
        return self._mask.source

    @property
    @_in_session()
    def value(self):
        """
        str: Value of the mask.
        """
        return self._mask.value

    @property
    @_in_session()
    def task(self):
        """
        :obj:`Task`: Task object corresponding to parent task.
        """
        if hasattr(self._mask.task, '_wpipe_object'):
            return self._mask.task._wpipe_object
        else:
            from .Task import Task
            return Task(self._mask.task)

    @property
    @_in_session()
    def task_id(self):
        """
        int: Primary key id of the table row of parent task.
        """
        return self._mask.task_id

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        si.delete(self._mask)
