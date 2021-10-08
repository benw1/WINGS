#!/usr/bin/env python
"""
Contains the Configuration class definition

Please note that this module is private. The Configuration class
is available in the main ``wpipe`` namespace - use that instead.
"""
from .core import os, datetime, pd, si
from .core import make_yield_session_if_not_cached, make_query_rtn_upd
from .core import initialize_args, wpipe_to_sqlintf_connection, in_session
from .core import remove_path, split_path
from .proxies import ChildrenProxy, DictLikeChildrenProxy
from .DPOwner import DPOwner

__all__ = ['Configuration']

KEYID_ATTR = 'config_id'
UNIQ_ATTRS = ['target_id', 'name']
CLASS_LOW = split_path(__file__)[1].lower()


def _in_session(**local_kw):
    return in_session('_%s' % CLASS_LOW, **local_kw)


_check_in_cache = make_yield_session_if_not_cached(KEYID_ATTR, UNIQ_ATTRS, CLASS_LOW)

_query_return_and_update_cached_row = make_query_rtn_upd(CLASS_LOW)


class Configuration(DPOwner):
    """
        Represents target's configuration.

        Call signatures::

            Configuration(target, name='default', description='',
                          parameters={})
            Configuration(keyid)
            Configuration(_configuration)

        When __new__ is called, it queries the database for an existing
        row in the `configurations` table via `sqlintf` using the given
        signature. If the row exists, it retrieves its corresponding
        `sqlintf.Configuration` object, otherwise it creates a new row via a
        new `sqlintf.Configuration` instance. This `sqlintf.Configuration`
        object is then wrapped under the hidden attribute
        `Configuration._configuration` in the new instance of this
        `Configuration` class generated by __new__.

        In the latter case where a new row is created, 4 directories are
        made in the data sub-directory of the parent target, respectively
        meant to receive raw data files (rawpath), configuration files
        (confpath), logging files (logpath) and processed data files
        (procpath). With the making of the sub-directories rawpath and
        confpath, symbolic links are created pointing to files of the parent
        input - while adding their corresponding rows associated with this
        configuration to the dataproducts table of the database - first in the
        rawpath, pointing to the raw data files, then in the confpath,
        pointing to the configuration file associated to this configuration.

        All configurations are uniquely identified by their target and their
        name, but alternatively, the constructor can take as sole argument
        either:
         - the primary key id of the corresponding `configurations` table row
         - the `sqlintf.Configuration` object interfacing that table row

        After the instantiation of __new__ is completed, if a dictionary of
        parameters was given to the constructor, the __init__ method
        constructs a set of Parameter objects owned by the configuration.

        Notes
        -----
        target : Target object
            Target owning this input.
        name : string
            Name of the configuration - defaults to 'default'.
        description : string
            Description of the configuration - defaults to ''.
        parameters : dict
            Dictionary of parameters to associate to the configuration
            - defaults to {}.
        keyid : int
            Primary key id of the table row.
        _configuration : sqlintf.Configuration object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Target object
            Points to attribute self.target.
        name : string
            Name of the configuration.
        config_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        datapath : string
            Path to the parent target data sub-directory.
        confpath : string
            Path to the configuration directory specific to configuration
            files.
        rawpath : string
            Path to the configuration directory specific to raw data files.
        logpath : string
            Path to the configuration directory specific to logging files.
        procpath : string
            Path to the configuration directory specific to processed data
            files.
        description : string
            Description of the configuration
        target_id : int
            Primary key id of the table row of parent target.
        target : Target object
            Target object corresponding to parent target.
        input_id: int
            Primary key id of the table row of parent input.
        input : Input object
            Input object corresponding to parent input.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        dummy_task : Task object
            Task object corresponding to the dummy task of the parent
            pipeline.
        parameters : core.DictLikeChildrenProxy object
            Dictionary of Parameter objects owned by the configuration.
        jobs : core.ChildrenProxy object
            List of Job objects owned by the configuration.
        dpowner_id : int
            Points to attribute configuration_id.
        rawdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the configuration
            corresponding to raw data files.
        confdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the configuration
            corresponding to configuration files.
        logdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the configuration
            corresponding to logging files.
        procdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the configuration
            corresponding to processed data files.
        dataproducts : core.ChildrenProxy object
            List of DataProduct objects owned by the configuration.

        Notes
        ----------
        Configuration objects are primarily associated to a single target from
        the pool of targets that the pipeline considers. Their construction is
        generally automatically processed when building that pool of targets
        from the input files that had been originally given to the pipeline.
        Also, one should access the existing Configuration objects from the
        corresponding Target object using their object property
        Target.configurations.

        Nonetheless, additional Configuration objects can be appended to those
        of a Target object, either by using the object method
        Target.configuration, or alternatively by using the Configuration
        class constructor giving it the Target object as argument. In both
        cases, a name must also be given:

        >>> my_config = my_target.configuration(name_of_config)
        or
        >>> my_config = wp.Configuration(my_target, name_of_config)
    """
    __cache__ = pd.DataFrame(columns=[KEYID_ATTR]+UNIQ_ATTRS+[CLASS_LOW])

    @classmethod
    def _check_in_cache(cls, kind, loc):
        return _check_in_cache(cls, kind, loc)

    @classmethod
    def _sqlintf_instance_argument(cls):
        if hasattr(cls, '_%s' % CLASS_LOW):
            for _session in cls._check_in_cache(kind='keyid',
                                                loc=getattr(cls, '_%s' % CLASS_LOW).get_id()):
                pass

    def __new__(cls, *args, **kwargs):
        if hasattr(cls, '_inst'):
            old_cls_inst = cls._inst
            delattr(cls, '_inst')
        else:
            old_cls_inst = None
        cls._to_cache = {}
        # checking if given argument is sqlintf object or existing id
        cls._configuration = args[0] if len(args) else None
        if not isinstance(cls._configuration, si.Configuration):
            keyid = kwargs.get('id', cls._configuration)
            if isinstance(keyid, int):
                for session in cls._check_in_cache(kind='keyid', loc=keyid):
                    cls._configuration = session.query(si.Configuration).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                target = kwargs.get('target', wpargs.get('Target', None))
                name = kwargs.get('name', 'default' if args[0] is None else args[0])
                description = kwargs.get('description', '' if args[1] is None else args[1])
                # pre-loading dataproducts to avoid extra-querying in the middle
                confdp = target.input.dataproduct(filename=name + '.conf', group='conf')
                rawdps = [rawdp for rawdp in target.input.rawdataproducts]
                # querying the database for existing row or create
                for session in cls._check_in_cache(kind='args', loc=(target.target_id, name)):
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._configuration = this_nested.session.query(si.Configuration).with_for_update(). \
                                filter_by(target_id=target.target_id). \
                                filter_by(name=name).one_or_none()
                            if cls._configuration is None:
                                cls._configuration = si.Configuration(name=name,
                                                                      datapath=target.datapath,
                                                                      confpath=target.datapath + '/conf_' + name,
                                                                      rawpath=target.datapath + '/raw_' + name,
                                                                      logpath=target.datapath + '/log_' + name,
                                                                      procpath=target.datapath + '/proc_' + name,
                                                                      description=description)
                                target._target.configurations.append(cls._configuration)
                                this_nested.commit()
                                if not os.path.isdir(cls._configuration.confpath):
                                    os.mkdir(cls._configuration.confpath)
                                if not os.path.isdir(cls._configuration.rawpath):
                                    os.mkdir(cls._configuration.rawpath)
                                if not os.path.isdir(cls._configuration.logpath):
                                    os.mkdir(cls._configuration.logpath)
                                if not os.path.isdir(cls._configuration.procpath):
                                    os.mkdir(cls._configuration.procpath)
                                with si.hold_commit():
                                    confdp.symlink(cls._configuration.confpath, return_dp=False,
                                                   dpowner=cls._configuration, group='conf')
                                    for rawdp in rawdps:
                                        rawdp.symlink(cls._configuration.rawpath, return_dp=False,
                                                      dpowner=cls._configuration, group='raw')
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        else:
            cls._sqlintf_instance_argument()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Configuration')
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
        if not hasattr(self, '_parameters_proxy'):
            self._parameters_proxy = DictLikeChildrenProxy(self._configuration, 'parameters', 'Parameter')
        self.parameters = kwargs.get('parameters', {})
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._configuration, 'jobs', 'Job',
                                             child_attr='id')
        if not hasattr(self, '_dpowner'):
            self._dpowner = self._configuration
        super(Configuration, self).__init__()

    @classmethod
    def select(cls, *args, **kwargs):
        """
        Returns a list of Configuration objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Configuration` for parameters.

        Returns
        -------
        out : list of Configuration object
            list of objects fulfilling the kwargs filter.
        """
        with si.begin_session() as session:
            cls._temp = session.query(si.Configuration).filter_by(**kwargs)
            for arg in args:
                cls._temp = cls._temp.filter(arg)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Target`: Points to attribute self.target.
        """
        return self.target

    @property
    @_in_session()
    def name(self):
        """
        str: Name of the configuration.
        """
        self._session.refresh(self._configuration)
        return _query_return_and_update_cached_row(self, 'name')

    @name.setter
    @_in_session()
    def name(self, name):
        self._configuration.name = name
        _temp = _query_return_and_update_cached_row(self, 'name')
        self.update_timestamp()
        # self._configuration.timestamp = datetime.datetime.utcnow()
        # self._session.commit()

    @property
    @_in_session()
    def config_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._configuration.id

    @property
    @_in_session()
    def datapath(self):
        """
        str: Path to the parent target data sub-directory.
        """
        return self._configuration.datapath

    @property
    @_in_session()
    def confpath(self):
        """
        str: Path to the configuration directory specific to configuration
        files.
        """
        return self._configuration.confpath

    @property
    @_in_session()
    def rawpath(self):
        """
        str: Path to the configuration directory specific to raw data files.
        """
        return self._configuration.rawpath

    @property
    @_in_session()
    def logpath(self):
        """
        str: Path to the configuration directory specific to logging files.
        """
        return self._configuration.logpath

    @property
    @_in_session()
    def procpath(self):
        """
        str: Path to the configuration directory specific to processed data
        files.
        """
        return self._configuration.procpath

    @property
    @_in_session()
    def description(self):
        """
        str: Description of the configuration
        """
        self._session.refresh(self._configuration)
        return self._configuration.description

    @description.setter
    @_in_session()
    def description(self, description):
        self._configuration.description = description
        self.update_timestamp()
        # self._configuration.timestamp = datetime.datetime.utcnow()
        # self._session.commit()

    @property
    @_in_session()
    def target_id(self):
        """
        int: Primary key id of the table row of parent target.
        """
        return self._configuration.target_id

    @property
    @_in_session()
    def target(self):
        """
        :obj:`Target`: Target object corresponding to parent target.
        """
        if hasattr(self._configuration.target, '_wpipe_object'):
            return self._configuration.target._wpipe_object
        else:
            from .Target import Target
            return Target(self._configuration.target)

    @property
    def input_id(self):
        """
        int: Primary key id of the table row of parent input.
        """
        return self.target.input_id

    @property
    def input(self):
        """
        :obj:`Input`: Input object corresponding to parent input.
        """
        return self.target.input

    @property
    def pipeline_id(self):
        """
        int: Primary key id of the table row of parent pipeline.
        """
        return self.target.pipeline_id

    @property
    def pipeline(self):
        """
        :obj:`Pipeline`: Pipeline object corresponding to parent pipeline.
        """
        return self.target.pipeline

    @property
    def dummy_task(self):
        """
        :obj:`Task`: Task object corresponding to the dummy task of the parent
        pipeline.
        """
        return self.pipeline.dummy_task

    @property
    def parameters(self):
        """
        :obj:`core.DictLikeChildrenProxy`: Dictionary of Parameter objects
        owned by the configuration.
        """
        return self._parameters_proxy

    @parameters.setter
    def parameters(self, parameters):
        for key, value in parameters.items():
            self.parameter(name=key, value=value)

    @property
    def jobs(self):
        """
        :obj:`core.ChildrenProxy`: List of Job objects owned by the
        configuration.
        """
        return self._jobs_proxy

    def parameter(self, *args, **kwargs):
        """
        Returns a parameter owned by the configuration.

        Parameters
        ----------
        kwargs
            Refer to :class:`Parameter` for parameters.

        Returns
        -------
        parameter : :obj:`Parameter`
            Parameter corresponding to given kwargs.
        """
        from .Parameter import Parameter
        return Parameter(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        """
        Returns a job owned by the configuration.

        Parameters
        ----------
        kwargs
            Refer to :class:`Job` for parameters.

        Returns
        -------
        job : :obj:`Job`
            Job corresponding to given kwargs.
        """
        from .Job import Job
        return Job(self, *args, **kwargs)

    def remove_data(self):
        """
        Remove pipeline's directories.
        """
        remove_path(self.confpath, self.rawpath, self.logpath, self.procpath)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        self.parameters.delete()
        self.jobs.delete()
        super(Configuration, self).delete(self.remove_data)
        self.__class__.__cache__ = self.__cache__[self.__cache__[CLASS_LOW] != self]
