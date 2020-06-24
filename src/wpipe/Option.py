#!/usr/bin/env python
"""
Contains the Option class definition

Please note that this module is private. The Option class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, si
from .core import initialize_args, wpipe_to_sqlintf_connection

__all__ = ['Option']


class Option:
    """
        Represents an option given to a target, job, event or dataproduct.

        Call signatures::

            Option(optowner, name, value)
            Option(keyid)
            Option(_option)

        When __new__ is called, it queries the database for an existing
        row in the `options` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Option`
        object, otherwise it creates a new row via a new `sqlintf.Option`
        instance. This `sqlintf.Option` object is then wrapped under the
        hidden attribute `Option._option` in the new instance of this `Option`
        class generated by __new__.

        All options are uniquely identified by their option owner and their
        name, but alternatively, the constructor can take as sole argument
        either:
         - the primary key id of the corresponding `options` table row
         - the `sqlintf.Option` object interfacing that table row

        Parameters
        ----------
        optowner : Target, Job, Event or DataProduct object
            Parent Target, Job, Event or DataProduct owning this option.
        name : string
            Name of the option.
        value : string
            Value of the option.
        keyid : int
            Primary key id of the table row.
        _option : sqlintf.Option object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Target, Job, Event or DataProduct object
            Points to attribute self.optowner.
        name : string
            Name of the option.
        option_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        value : string
            Value of the option.
        optowner : Target, Job, Event or DataProduct object
            Target, Job, Event or DataProduct object corresponding to parent
            optowner.
        optowner_id : int
            Primary key id of the table row of parent optowner.
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._option = args[0] if len(args) else None
        if not isinstance(cls._option, si.Option):
            keyid = kwargs.get('id', cls._option)
            if isinstance(keyid, int):
                cls._option = si.session.query(si.Option).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                list(wpargs.__setitem__('OptOwner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.OptOwner.__subclasses__())))
                optowner = kwargs.get('optowner', wpargs.get('OptOwner', None))
                name = kwargs.get('name', args[0])
                value = kwargs.get('value', args[1])
                # querying the database for existing row or create
                try:
                    cls._option = si.session.query(si.Option). \
                        filter_by(optowner_id=optowner.optowner_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._option = si.Option(name=name,
                                            value=str(value))
                    optowner._optowner.options.append(cls._option)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Option')
        return cls._inst

    def __init__(self, *args, **kwargs):
        self._option.timestamp = datetime.datetime.utcnow()
        si.commit()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Option objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Option` for parameters.

        Returns
        -------
        out : list of Option object
            list of objects fulfilling the kwargs filter.
        """
        cls._temp = si.session.query(si.Option).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Target`, :obj:`Job`, :obj:`Event` or :obj:`DataProduct`: Points
        to attribute self.optowner.
        """
        return self.optowner

    @property
    def name(self):
        """
        str: Name of the option.
        """
        si.commit()
        return self._option.name

    @name.setter
    def name(self, name):
        self._option.name = name
        self._option.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def option_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._option.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        si.commit()
        return self._option.timestamp

    @property
    def value(self):
        """
        str: Value of the option.
        """
        si.commit()
        return self._option.value

    @value.setter
    def value(self, value):
        self._option.value = value
        self._option.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def optowner(self):
        """
        :obj:`Target`, :obj:`Job`, :obj:`Event` or :obj:`DataProduct`: Target,
        Job, Event or DataProduct object corresponding to parent optowner.
        """
        if hasattr(self._option.optowner, '_wpipe_object'):
            return self._option.optowner._wpipe_object
        else:
            if self._option.optowner.type == 'target':
                from .Target import Target
                return Target(self._option.optowner)
            elif self._option.optowner.type == 'job':
                from .Job import Job
                return Job(self._option.optowner)
            elif self._option.optowner.type == 'event':
                from .Event import Event
                return Event(self._option.optowner)
            elif self._option.optowner.type == 'dataproduct':
                from .DataProduct import DataProduct
                return DataProduct(self._option.optowner)

    @property
    def optowner_id(self):
        """
        int: Primary key id of the table row of parent optowner.
        """
        return self._option.optowner_id

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        si.session.delete(self._option)
        si.commit()
