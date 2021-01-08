#!/usr/bin/env python
"""
Contains the Mask class definition

Please note that this module is private. The Mask class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, si
from .core import initialize_args, wpipe_to_sqlintf_connection

__all__ = ['Mask']


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
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._mask = args[0] if len(args) else None
        if not isinstance(cls._mask, si.Mask):
            keyid = kwargs.get('id', cls._mask)
            if isinstance(keyid, int):
                cls._mask = si.session.query(si.Mask).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=3)
                task = kwargs.get('task', wpargs.get('Task', None))
                name = kwargs.get('name', args[0])
                source = kwargs.get('source', '' if args[1] is None else args[1])
                value = kwargs.get('value', '' if args[2] is None else args[2])
                # querying the database for existing row or create
                for retry in si.retrying_nested():
                    with retry:
                        this_nested = si.begin_nested()
                        try:
                            cls._mask = si.session.query(si.Mask).with_for_update(). \
                                filter_by(task_id=task.task_id). \
                                filter_by(name=name).one()
                            this_nested.rollback()
                        except si.orm.exc.NoResultFound:
                            cls._mask = si.Mask(name=name,
                                                source=source,
                                                value=value)
                            task._task.masks.append(cls._mask)
                            this_nested.commit()
                        retry.retry_state.commit()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Mask')
        return cls._inst

    def __init__(self, *args, **kwargs):
        self._mask.timestamp = datetime.datetime.utcnow()
        si.commit()

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
        cls._temp = si.session.query(si.Mask).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Task`: Points to attribute self.task.
        """
        return self.task

    @property
    def name(self):
        """
        str: Name of the mask.
        """
        si.refresh(self._mask)
        return self._mask.name

    @name.setter
    def name(self, name):
        self._mask.name = name
        self._mask.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def mask_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._mask.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        si.refresh(self._mask)
        return self._mask.timestamp

    @property
    def source(self):
        """
        str: Source of the mask.
        """
        return self._mask.source

    @property
    def value(self):
        """
        str: Value of the mask.
        """
        return self._mask.value

    @property
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
    def task_id(self):
        """
        int: Primary key id of the table row of parent task.
        """
        return self._mask.task_id

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        si.session.delete(self._mask)
        si.commit()
