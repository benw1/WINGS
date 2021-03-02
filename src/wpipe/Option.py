#!/usr/bin/env python
"""
Contains the Option class definition

Please note that this module is private. The Option class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, pd, si
from .core import make_yield_session_if_not_cached, initialize_args, wpipe_to_sqlintf_connection, in_session
from .core import split_path

__all__ = ['Option']

KEYID_ATTR = 'option_id'
UNIQ_ATTRS = ['optowner_id', 'name']
CLASS_LOW = split_path(__file__)[1].lower()


def _in_session(**local_kw):
    return in_session('_%s' % CLASS_LOW, **local_kw)


_check_in_cache = make_yield_session_if_not_cached(KEYID_ATTR, UNIQ_ATTRS, CLASS_LOW)


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
        cls._option = args[0] if len(args) else None
        if not isinstance(cls._option, si.Option):
            keyid = kwargs.get('id', cls._option)
            if isinstance(keyid, int):
                for session in cls._check_in_cache(kind='keyid', loc=keyid):
                    cls._option = session.query(si.Option).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                list(wpargs.__setitem__('OptOwner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.OptOwner.__subclasses__())))
                optowner = kwargs.get('optowner', wpargs.get('OptOwner', None))
                name = kwargs.get('name', args[0])
                value = kwargs.get('value', args[1])
                # querying the database for existing row or create
                for session in cls._check_in_cache(kind='args', loc=(optowner.optowner_id, name)):
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._option = this_nested.session.query(si.Option).with_for_update(). \
                                filter_by(optowner_id=optowner.optowner_id). \
                                filter_by(name=name).one_or_none()
                            if cls._option is None:
                                cls._option = si.Option(name=name,
                                                        value=str(value))
                                optowner._optowner.options.append(cls._option)
                                this_nested.commit()
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        else:
            with si.begin_session() as session:
                session.add(cls._option)
                for _session in cls._check_in_cache(kind='keyid', loc=cls._option.id):
                    pass
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Option')
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
        self._option.timestamp = datetime.datetime.utcnow()
        self._session.commit()

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
        with si.begin_session() as session:
            cls._temp = session.query(si.Option).filter_by(**kwargs)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Target`, :obj:`Job`, :obj:`Event` or :obj:`DataProduct`: Points
        to attribute self.optowner.
        """
        return self.optowner

    @property
    @_in_session()
    def name(self):
        """
        str: Name of the option.
        """
        self._session.refresh(self._option)
        return self._option.name

    @name.setter
    @_in_session()
    def name(self, name):
        self._option.name = name
        self._option.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
    def option_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._option.id

    @property
    @_in_session()
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        self._session.refresh(self._option)
        return self._option.timestamp

    @property
    @_in_session()
    def value(self):
        """
        str: Value of the option.
        """
        self._session.refresh(self._option)
        return self._option.value

    @value.setter
    @_in_session()
    def value(self, value):
        self._option.value = value
        self._option.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
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
    @_in_session()
    def optowner_id(self):
        """
        int: Primary key id of the table row of parent optowner.
        """
        return self._option.optowner_id

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        si.delete(self._option)
