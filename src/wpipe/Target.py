#!/usr/bin/env python
"""
Contains the Target class definition

Please note that this module is private. The Target class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import os, datetime, json, si
from .core import ChildrenProxy
from .core import initialize_args, wpipe_to_sqlintf_connection, in_session
from .core import remove_path, split_path
from .OptOwner import OptOwner

__all__ = ['Target']


def _in_session(**local_kw):
    return in_session('_%s' %  split_path(__file__)[1].lower(), **local_kw)


class Target(OptOwner):
    """
        Represents an input's target.

        Call signatures::

            Target(input, name=input.name, options={})
            Target(keyid)
            Target(_target)

        When __new__ is called, it queries the database for an existing
        row in the `targets` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Target`
        object, otherwise it creates a new row via a new `sqlintf.Target`
        instance. This `sqlintf.Target` object is then wrapped under the
        hidden attribute `Target._target` in the new instance of this `Target`
        class generated by __new__.

        In the latter case where a new row is created, a directory is made
        in the directory data_root of the pipeline named after the target
        name.

        All targets are uniquely identified by their input and their name,
        but alternatively, the constructor can take as sole argument either:
         - the primary key id of the corresponding `targets` table row
         - the `sqlintf.Target` object interfacing that table row

        After the instantiation of __new__ is completed, the __init__ method
        constructs and assigns the Configuration object corresponding to the
        default configuration. Then, it calls the object method
        configure_target to parametrize the set of configurations owned by the
        target accordingly with the configuration files of the parent input.
        Lastly, if a dictionary of options was given to the constructor, it
        constructs a set of Option objects owned by the target.

        Parameters
        ----------
        input : Input object
            Input owning this target.
        name : string
            Name of the target - defaults to input.name.
        options : dict
            Dictionary of options to associate to the target - defaults to {}.
        keyid : int
            Primary key id of the table row.
        _target : sqlintf.Target object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Input object
            Points to attribute self.input.
        name : string
            Name of the target.
        target_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        datapath : string
            Path to the target data sub-directory.
        dataraws : string
            Path to the target parent input directory specific to raw data
            files.
        input_id : int
            Primary key id of the table row of parent input.
        input : Input object
            Input object corresponding to parent input.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        configurations : core.ChildrenProxy object
            List of Configuration objects owned by the target.
        default_conf : Configuration object
            Configuration object corresponding to a default configuration
            for the target.
        optowner_id : int
            Points to attribute target_id.
        options : core.DictLikeChildrenProxy object
            Dictionary of Option objects owned by the target.

        Notes
        -----
        As a Target object requires an Input object to construct, one can
        either use the Input object method target, or use the Target class
        constructor with the Input object as argument. In both cases, the
        argument name can be provided to specify a name, otherwise the name
        attribute of the Input object will be used.

        >>> my_target = my_input.target()
        or
        >>> my_target = wp.Target(my_input)
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._target = args[0] if len(args) else None
        if not isinstance(cls._target, si.Target):
            keyid = kwargs.get('id', cls._target)
            if isinstance(keyid, int):
                with si.begin_session() as session:
                    cls._target = session.query(si.Target).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                input = kwargs.get('input', wpargs.get('Input', None))
                name = kwargs.get('name', input.name if args[0] is None else args[0])
                # querying the database for existing row or create
                with si.begin_session() as session:
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._target = this_nested.session.query(si.Target).with_for_update(). \
                                filter_by(input_id=input.input_id). \
                                filter_by(name=name).one_or_none()
                            if cls._target is None:
                                with si.hold_commit():
                                    cls._target = si.Target(name=name,
                                                            datapath=input.pipeline.data_root+'/'+name,
                                                            dataraws=input.rawspath)
                                    input._input.targets.append(cls._target)
                                    this_nested.commit()
                                if not os.path.isdir(cls._target.datapath):
                                    os.mkdir(cls._target.datapath)
                                if not os.path.isdir(cls._target.dataraws):
                                    os.mkdir(cls._target.dataraws)
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Target')
        return cls._inst

    @_in_session()
    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_configurations_proxy'):
            self._configurations_proxy = ChildrenProxy(self._target, 'configurations', 'Configuration')
        if not hasattr(self, '_optowner'):
            self._optowner = self._target
        if not hasattr(self, '_default_conf'):
            self._default_conf = self.configuration()
        self.configure_target()
        super(Target, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Target objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Target` for parameters.

        Returns
        -------
        out : list of Target object
            list of objects fulfilling the kwargs filter.
        """
        with si.begin_session() as session:
            cls._temp = session.query(si.Target).filter_by(**kwargs)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Input`: Points to attribute self.input.
        """
        return self.input

    @property
    @_in_session()
    def name(self):
        """
        str: Name of the target.
        """
        self._session.refresh(self._target)
        return self._target.name

    @name.setter
    @_in_session()
    def name(self, name):
        self._target.name = name
        self._target.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
    def target_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._target.id

    @property
    @_in_session()
    def datapath(self):
        """
        str: Path to the target data sub-directory.
        """
        return self._target.datapath

    @property
    @_in_session()
    def dataraws(self):
        """
        str: Path to the target parent input directory specific to raw data
        files.
        """
        return self._target.dataraws

    @property
    @_in_session()
    def input_id(self):
        """
        int: Primary key id of the table row of parent input.
        """
        return self._target.input_id

    @property
    @_in_session()
    def input(self):
        """
        :obj:`Input`: Input object corresponding to parent input.
        """
        if hasattr(self._target.input, '_wpipe_object'):
            return self._target.input._wpipe_object
        else:
            from .Input import Input
            return Input(self._target.input)

    @property
    def pipeline_id(self):
        """
        int: Primary key id of the table row of parent pipeline.
        """
        return self.input.pipeline_id

    @property
    def pipeline(self):
        """
        :obj:`Pipeline`: Pipeline object corresponding to parent pipeline.
        """
        return self.input.pipeline

    @property
    def configurations(self):
        """
        :obj:`core.ChildrenProxy`: List of Configuration objects owned by the
        target.
        """
        return self._configurations_proxy

    @property
    def default_conf(self):
        """
        :obj:`Configuration`: Configuration object corresponding to a default
        configuration for the target.
        """
        return self._default_conf

    def configuration(self, *args, **kwargs):
        """
        Returns a configuration owned by the target.

        Parameters
        ----------
        kwargs
            Refer to :class:`Configuration` for parameters.

        Returns
        -------
        configuration : :obj:`Configuration`
            Configuration corresponding to given kwargs.
        """
        from .Configuration import Configuration
        return Configuration(self, *args, **kwargs)

    def configure_target(self):
        """
        Generate configurations for each conf dataproduct owned by parent
        input.
        """
        for confdp in self.input.confdataproducts:
            self.configuration(os.path.splitext(confdp.filename)[0],
                               parameters=json.load(open(confdp.relativepath+'/'+confdp.filename))[0])

    def remove_data(self):
        """
        Remove target's directories.
        """
        remove_path(self.datapath)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        self.configurations.delete()
        self.remove_data()
        super(Target, self).delete()
