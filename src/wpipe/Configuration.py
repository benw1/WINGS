#!/usr/bin/env python
"""
Contains the Configuration class definition

Please note that this module is private. The Configuration class
is available in the main ``wpipe`` namespace - use that instead.
"""
from .core import *
from .DPOwner import DPOwner


class Configuration(DPOwner):
    """
        A Configuration object represents a set of parameters configuring
        a target, and is defined by its target and its name.

        Construction
        ------------

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

        Parameters
        ----------
        target : Target object
            Target owning this input.
        name : string
            Name of the configuration - defaults to 'default'.
        description : 'string'
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

        How to use
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
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._configuration = args[0] if len(args) else None
        if not isinstance(cls._configuration, si.Configuration):
            keyid = kwargs.get('id', cls._configuration)
            if isinstance(keyid, int):
                cls._configuration = si.session.query(si.Configuration).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                target = kwargs.get('target', wpargs.get('Target', None))
                name = kwargs.get('name', 'default' if args[0] is None else args[0])
                description = kwargs.get('description', '' if args[1] is None else args[1])
                # querying the database for existing row or create
                try:
                    cls._configuration = si.session.query(si.Configuration). \
                        filter_by(target_id=target.target_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._configuration = si.Configuration(name=name,
                                                          datapath=target.datapath,
                                                          confpath=target.datapath + '/conf_' + name,
                                                          rawpath=target.datapath + '/raw_' + name,
                                                          logpath=target.datapath + '/log_' + name,
                                                          procpath=target.datapath + '/proc_' + name,
                                                          description=description)
                    target._target.configurations.append(cls._configuration)
                    if not os.path.isdir(cls._configuration.confpath):
                        os.mkdir(cls._configuration.confpath)
                    confdp = target.input.dataproduct(filename=name+'.conf', group='conf')
                    confdp.symlink(cls._configuration.confpath, dpowner=cls._configuration, group='conf')
                    if not os.path.isdir(cls._configuration.rawpath):
                        os.mkdir(cls._configuration.rawpath)
                    for rawdp in target.input.rawdataproducts:
                        rawdp.symlink(cls._configuration.rawpath, dpowner=cls._configuration, group='raw')
                    if not os.path.isdir(cls._configuration.logpath):
                        os.mkdir(cls._configuration.logpath)
                    if not os.path.isdir(cls._configuration.procpath):
                        os.mkdir(cls._configuration.procpath)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Configuration')
        return cls._inst

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
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Configuration).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.target

    @property
    def name(self):
        si.session.commit()
        return self._configuration.name

    @name.setter
    def name(self, name):
        self._configuration.name = name
        self._configuration.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config_id(self):
        si.session.commit()
        return self._configuration.id

    @property
    def datapath(self):
        si.session.commit()
        return self._configuration.datapath

    @property
    def confpath(self):
        si.session.commit()
        return self._configuration.confpath

    @property
    def rawpath(self):
        si.session.commit()
        return self._configuration.rawpath

    @property
    def logpath(self):
        si.session.commit()
        return self._configuration.logpath

    @property
    def procpath(self):
        si.session.commit()
        return self._configuration.procpath

    @property
    def description(self):
        si.session.commit()
        return self._configuration.description

    @description.setter
    def description(self, description):
        self._configuration.description = description
        self._configuration.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def target_id(self):
        si.session.commit()
        return self._configuration.target_id

    @property
    def target(self):
        if hasattr(self._configuration.target, '_wpipe_object'):
            return self._configuration.target._wpipe_object
        else:
            from .Target import Target
            return Target(self._configuration.target)

    @property
    def input_id(self):
        return self.target.input_id

    @property
    def input(self):
        return self.target.input

    @property
    def pipeline_id(self):
        return self.target.pipeline_id

    @property
    def pipeline(self):
        return self.target.pipeline

    @property
    def dummy_task(self):
        return self.pipeline.dummy_task

    @property
    def parameters(self):
        return self._parameters_proxy

    @parameters.setter
    def parameters(self, parameters):
        for key, value in parameters.items():
            self.parameter(name=key, value=value)

    @property
    def jobs(self):
        return self._jobs_proxy

    def parameter(self, *args, **kwargs):
        from .Parameter import Parameter
        return Parameter(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        from .Job import Job
        return Job(self, *args, **kwargs)
