#!/usr/bin/env python
"""
Contains the Parameter class definition

Please note that this module is private. The Parameter class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, si
from .core import initialize_args, wpipe_to_sqlintf_connection

__all__ = ['Parameter']


class Parameter:
    """
        Represents a configuration's parameter.

        Call signatures::

            Parameter(configuration, name, value)
            Parameter(keyid)
            Parameter(_parameter)

        When __new__ is called, it queries the database for an existing
        row in the `parameters` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Parameter`
        object, otherwise it creates a new row via a new `sqlintf.Parameter`
        instance. This `sqlintf.Parameter` object is then wrapped under the
        hidden attribute `Parameter._parameter` in the new instance of this
        `Parameter` class generated by __new__.

        All parameters are uniquely identified by their configuration and
        their name, but alternatively, the constructor can take as sole
        argument either:
         - the primary key id of the corresponding `parameters` table row
         - the `sqlintf.Parameter` object interfacing that table row

        Parameters
        ----------
        configuration : Configuration object
            Parent Configuration owning this option.
        name : string
            Name of the parameter.
        value : string
            Value of the parameter.
        keyid : int
            Primary key id of the table row.
        _parameter : sqlintf.Parameter object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Configuration object
            Points to attribute self.config.
        name : string
            Name of the parameter.
        parameter_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        value : string
            Value of the parameter.
        config : Configuration object
            Configuration object corresponding to parent configuration.
        config_id : int
            Primary key id of the table row of parent configuration.
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._parameter = args[0] if len(args) else None
        if not isinstance(cls._parameter, si.Parameter):
            keyid = kwargs.get('id', cls._parameter)
            if isinstance(keyid, int):
                cls._parameter = si.session.query(si.Parameter).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                config = kwargs.get('config', wpargs.get('Configuration', None))
                name = kwargs.get('name', args[0])
                value = kwargs.get('value', args[1])
                # querying the database for existing row or create
                try:
                    cls._parameter = si.session.query(si.Parameter). \
                        filter_by(config_id=config.config_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._parameter = si.Parameter(name=name,
                                                  value=str(value))
                    config._configuration.parameters.append(cls._parameter)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Parameter')
        return cls._inst

    def __init__(self, *args, **kwargs):
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Parameter objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs : dict
            Refer to :class:`sqlintf.Parameter` for parameters.

        Returns
        -------
        out : list of Parameter object
            list of objects fulfilling the kwargs filter.
        """
        cls._temp = si.session.query(si.Parameter).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Configuration`: Points to attribute self.config.
        """
        return self.config

    @property
    def name(self):
        """
        str: Name of the parameter.
        """
        return self._parameter.name

    @name.setter
    def name(self, name):
        self._parameter.name = name
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parameter_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._parameter.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        si.session.commit()
        return self._parameter.timestamp

    @property
    def value(self):
        """
        str: Value of the parameter.
        """
        si.session.commit()
        return self._parameter.value

    @value.setter
    def value(self, value):
        self._parameter.value = value
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config(self):
        """
        :obj:`Configuration`: Configuration object corresponding to parent
        configuration.
        """
        if hasattr(self._parameter.config, '_wpipe_object'):
            return self._parameter.config._wpipe_object
        else:
            from .Configuration import Configuration
            return Configuration(self._parameter.config)

    @property
    def config_id(self):
        """
        int: Primary key id of the table row of parent configuration.
        """
        return self._parameter.config_id
