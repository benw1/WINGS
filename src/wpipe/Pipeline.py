#!/usr/bin/env python
"""
Contains the Pipeline class definition

Please note that this module is private. The Pipeline class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import os, sys, glob, shutil, datetime, json, si
from .core import ChildrenProxy
from .core import to_json, initialize_args, wpipe_to_sqlintf_connection, as_int, clean_path, remove_path
from .core import PARSER
from .DPOwner import DPOwner

__all__ = ['Pipeline']


class Pipeline(DPOwner):
    """
        Represents a WINGS pipeline.

        Call signatures::

            Pipeline(user=DefaultUser, pipe_root=PARSER.pipeline,
                     name=os.path.basename(pipe_root), software_root='build',
                     input_root='input', data_root='data',
                     config_root='config', description='')
            Pipeline(keyid)
            Pipeline(_pipeline)

        When __new__ is called, it queries the database for an existing
        row in the `pipelines` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Pipeline`
        object, otherwise it creates a new row via a new `sqlintf.Pipeline`
        instance. This `sqlintf.Pipeline` object is then wrapped under the
        hidden attribute `Pipeline._pipeline` in the new instance of this
        `Pipeline` class generated by __new__.

        In the latter case where a new row is created, 4 directories are
        made in the directory of the pipeline, respectively named after
        the parameters software_root, input_root, data_root and config_root.
        With the making of the sub-directory software_root, an __init__.py
        file is generated within that directory which corresponds to a dummy
        task to be used as a default task of the pipeline if necessary.

        All pipelines are uniquely identified by their user and their
        pipe_root, but alternatively, the constructor can take as sole
        argument either:
         - the primary key id of the corresponding `pipelines` table row
         - the `sqlintf.Pipeline` object interfacing that table row

        After the instantiation of __new__ is completed, the __init__ method
        constructs and assigns the Task object corresponding to the
        __init__.py dummy routine in software_root, and the dummy Job object
        that can start the pipeline run.

        Parameters
        ----------
        user : User object
            User owning this pipeline - defaults to DefaultUser.
        pipe_root : string
            Path to the pipeline directory - defaults to PARSER.pipeline
            (see Notes).
        name : string
            Name of the pipeline - defaults to the string returned
            by os.path.basename(pipe_root).
        software_root : string
            Elect name for the sub-directory where the software routines
            will be stored - defaults to 'build'
        input_root : string
            Elect name for the sub-directory where the input data will
            be stored - defaults to 'input'
        data_root : string
            Elect name for the sub-directory where the other data will
            be stored - defaults to 'data'
        config_root : string
            Elect name for the sub-directory where the configurations
            will be stored - defaults to 'config'
        description : string
            Description of the pipeline - defaults to empty string ''
        keyid : int
            Primary key id of the table row.
        _pipeline : sqlintf.Pipeline object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : User object
            Points to attribute self.user.
        name : string
            Name of the pipeline.
        pipeline_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        pipe_root : string
            Path to the pipeline directory.
        software_root : string
            Elect name for the sub-directory where the software routines
            will be stored.
        input_root : string
            Elect name for the sub-directory where the input data will
            be stored.
        data_root : string
            Elect name for the sub-directory where the other data will
            be stored.
        config_root : string
            Elect name for the sub-directory where the configurations
            will be stored.
        description : string
            Description of the pipeline.
        user_id : int
            Primary key id of the table row of parent user.
        user_name : string
            Name of parent user.
        user : User object
            User object corresponding to parent user.
        inputs : core.ChildrenProxy object
            List of Input objects owned by the pipeline.
        tasks : core.ChildrenProxy object
            List of Task objects owned by the pipeline.
        dummy_task : Task object
            Task object corresponding to the dummy __init__.py routine
            in software_root.
        dummy_job : Job object
            Dummy Job object for starting the pipeline.
        dpowner_id : int
            Points to attribute pipeline_id.
        rawdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the pipeline corresponding
            to raw data files.
        confdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the pipeline corresponding
            to configuration files.
        logdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the pipeline corresponding
            to logging files.
        procdataproducts : list of DataProduct objects
            List of DataProduct objects owned by the pipeline corresponding
            to processed data files.
        dataproducts : core.ChildrenProxy object
            List of DataProduct objects owned by the pipeline.

        Notes
        -----
        PARSER.pipeline can be configured through the parse argument
        -p/--pipeline, but it defaults to the current working directory
        returned by os.cwd().

        A Pipeline object can be constructed without giving any parameter:
        in such case, it is automatically associated to the user DefaultUser
        and the pipe_root is given by the PARSER.pipeline entry.

        >>> my_pipe = wp.Pipeline()
        >>> my_pipe.user_id == wp.DefaultUser.user_id
        True

        The newly constructed Pipeline object comes with 3 methods that
        are integral to the functioning of wpipe. The first one called
        attach_tasks builds the pipeline software suite of Task objects
        given the directory path where the original software files are
        located. The second one called attach_inputs assembles the collection
        of Input objects representing the input data given the directory path
        where the raw data are located as well as an optional path to a
        configuration file. The third and last one called run_pipeline
        simply starts the pipeline run.
    """

    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._pipeline = args[0] if len(args) else as_int(PARSER.parse_known_args()[0].pipeline)
        if not isinstance(cls._pipeline, si.Pipeline):
            if isinstance(cls._pipeline, str):
                if os.path.isfile(cls._pipeline + '/.wpipe/pipe.conf'):
                    with open(cls._pipeline + '/.wpipe/pipe.conf', 'r') as jsonfile:
                        cls._pipeline = int(json.load(jsonfile)[0]['id'])
            keyid = kwargs.get('id', cls._pipeline)
            if isinstance(keyid, int):
                with si.begin_session() as session:
                    try:
                        cls._pipeline = session.query(si.Pipeline).filter_by(id=keyid).one()
                    except si.orm.exc.NoResultFound:
                        raise (si.orm.exc.NoResultFound(
                            "No row was found for one(): make sure the .wpipe/ directory was removed"))
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=8)
                from . import DefaultUser
                user = kwargs.get('user', wpargs.get('User', DefaultUser))
                pipe_root = clean_path(kwargs.get('pipe_root',
                                                  PARSER.parse_known_args()[0].pipeline if args[0] is None
                                                  else args[0]))
                name = kwargs.get('name', os.path.basename(pipe_root) if args[1] is None else args[1])
                software_root = clean_path(kwargs.get('software_root',
                                                      'build' if args[2] is None else args[2]),
                                           root=pipe_root)
                input_root = clean_path(kwargs.get('input_root',
                                                   'input' if args[3] is None else args[3]),
                                        root=pipe_root)
                data_root = clean_path(kwargs.get('data_root',
                                                  'data' if args[4] is None else args[4]),
                                       root=pipe_root)
                config_root = clean_path(kwargs.get('config_root',
                                                    'config' if args[5] is None else args[5]),
                                         root=pipe_root)
                description = kwargs.get('description',
                                         '' if args[6] is None else args[6])
                # querying the database for existing row or create
                with si.begin_session() as session:
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            try:
                                cls._pipeline = this_nested.session.query(si.Pipeline).with_for_update(). \
                                    filter_by(user_id=user.user_id). \
                                    filter_by(pipe_root=pipe_root).one()
                                this_nested.rollback()
                            except si.orm.exc.NoResultFound:
                                cls._pipeline = si.Pipeline(name=name,
                                                            pipe_root=pipe_root,
                                                            software_root=software_root,
                                                            input_root=input_root,
                                                            data_root=data_root,
                                                            config_root=config_root,
                                                            description=description)
                                user._user.pipelines.append(cls._pipeline)
                                this_nested.commit()
                                if not os.path.isdir(cls._pipeline.pipe_root + '/.wpipe'):
                                    os.mkdir(cls._pipeline.pipe_root + '/.wpipe')
                                if not os.path.isfile(cls._pipeline.pipe_root + '/.wpipe/pipe.conf'):
                                    to_json(cls._pipeline, cls._pipeline.pipe_root + '/.wpipe/pipe.conf', orient='records')
                                cls._pipeline.dataproducts.append(si.DataProduct(filename='pipe.conf',
                                                                                 group='conf',
                                                                                 relativepath=cls._pipeline.pipe_root +
                                                                                              '/.wpipe'))
                                if not os.path.isdir(cls._pipeline.software_root):
                                    os.mkdir(cls._pipeline.software_root)
                                if not os.path.isfile(cls._pipeline.software_root + '/__init__.py'):
                                    with open(cls._pipeline.software_root + '/__init__.py', 'w') as file:
                                        file.write("def register(task):\n    return")
                                if not os.path.isdir(cls._pipeline.input_root):
                                    os.mkdir(cls._pipeline.input_root)
                                if not os.path.isdir(cls._pipeline.data_root):
                                    os.mkdir(cls._pipeline.data_root)
                                if not os.path.isdir(cls._pipeline.config_root):
                                    os.mkdir(cls._pipeline.config_root)
                            retry.retry_state.commit()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Pipeline')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if self.pipe_root not in map(os.path.abspath, sys.path):
            sys.path.insert(0, self.pipe_root)
        if not hasattr(self, '_inputs_proxy'):
            self._inputs_proxy = ChildrenProxy(self._pipeline, 'inputs', 'Input')
        if not hasattr(self, '_tasks_proxy'):
            self._tasks_proxy = ChildrenProxy(self._pipeline, 'tasks', 'Task')
        if not hasattr(self, '_dpowner'):
            self._dpowner = self._pipeline
        if not hasattr(self, '_dummy_task'):
            self._dummy_task = self.task(self.software_root + '/__init__.py')
        if not hasattr(self, '_dummy_job'):
            self._dummy_job = self.dummy_task.job()
        super(Pipeline, self).__init__()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Pipeline objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Pipeline` for parameters.

        Returns
        -------
        out : list of Pipeline object
            list of objects fulfilling the kwargs filter.
        """
        with si.begin_session() as session:
            cls._temp = session.query(si.Pipeline).filter_by(**kwargs)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`User`: Points to attribute self.user.
        """
        return self.user

    @property
    def name(self):
        """
        str: Name of the pipeline.
        """
        with si.begin_session() as session:
            session.refresh(self._pipeline)
        return self._pipeline.name

    @name.setter
    def name(self, name):
        with si.begin_session() as session:
            self._pipeline.name = name
            self._pipeline.timestamp = datetime.datetime.utcnow()
            session.commit()

    @property
    def pipeline_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._pipeline.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        with si.begin_session() as session:
            session.refresh(self._pipeline)
        return self._pipeline.timestamp

    @property
    def pipe_root(self):
        """
        str: Path to the pipeline directory.
        """
        return self._pipeline.pipe_root

    @property
    def software_root(self):
        """
        str: Elect name for the sub-directory where the software routines
        will be stored.
        """
        return self._pipeline.software_root

    @property
    def input_root(self):
        """
        str: Elect name for the sub-directory where the input data will be
        stored.
        """
        return self._pipeline.input_root

    @property
    def data_root(self):
        """
        str: Elect name for the sub-directory where the other data will be
        stored.
        """
        return self._pipeline.data_root

    @property
    def config_root(self):
        """
        str: Elect name for the sub-directory where the configurations will
        be stored.
        """
        return self._pipeline.config_root

    @property
    def description(self):
        """
        str: Description of the pipeline.
        """
        with si.begin_session() as session:
            session.refresh(self._pipeline)
        return self._pipeline.description

    @description.setter
    def description(self, description):
        with si.begin_session() as session:
            self._pipeline.description = description
            self._pipeline.timestamp = datetime.datetime.utcnow()
            session.commit()

    @property
    def user_id(self):
        """
        int: Primary key id of the table row of parent user.
        """
        return self._pipeline.user_id

    @property
    def user_name(self):
        """
        str: Name of parent user.
        """
        return self._pipeline.user.name

    @property
    def user(self):
        """
        :obj:`User`: User object corresponding to parent user.
        """
        if hasattr(self._pipeline.user, '_wpipe_object'):
            return self._pipeline.user._wpipe_object
        else:
            from .User import User
            return User(self._pipeline.user)

    @property
    def inputs(self):
        """
        :obj:`core.ChildrenProxy`: List of Input objects owned by the
        pipeline.
        """
        return self._inputs_proxy

    @property
    def tasks(self):
        """
        :obj:`core.ChildrenProxy`: List of Task objects owned by the pipeline.
        """
        return self._tasks_proxy

    @property
    def nondummy_tasks(self):
        """
        list of :obj:`DataProduct`: List of other Task objects owned by the
        pipeline that are not dummy.
        """
        return self.tasks[self.tasks.name != '__init__.py']

    @property
    def dummy_task(self):
        """
        :obj:`Task`: Task object corresponding to the dummy __init__.py
        routine in software_root.
        """
        return self._dummy_task

    @property
    def dummy_job(self):
        """
        :obj:`Job`: Dummy Job object for starting the pipeline.
        """
        return self._dummy_job

    def input(self, *args, **kwargs):
        """
        Returns an input owned by the pipeline.

        Parameters
        ----------
        kwargs
            Refer to :class:`Input` for parameters.

        Returns
        -------
        input : :obj:`Input`
            Input corresponding to given kwargs.
        """
        from .Input import Input
        return Input(self, *args, **kwargs)

    def task(self, *args, **kwargs):
        """
        Returns a task owned by the pipeline.

        Parameters
        ----------
        kwargs
            Refer to :class:`Task` for parameters.

        Returns
        -------
        task : :obj:`Task`
            Task corresponding to given kwargs.
        """
        from .Task import Task
        return Task(self, *args, **kwargs)

    def attach_tasks(self, tasks_path):
        """
        Explore the given path and register task for each script found.

        Parameters
        ----------
        tasks_path : str
            Path to root where task scripts are located.
        """
        tasks_path = clean_path(tasks_path)
        if tasks_path is not None:
            for task_path in glob.glob(tasks_path + '/*'):
                if os.path.isfile(task_path) and os.access(task_path, os.X_OK):
                    self.task(task_path).register()

    def attach_inputs(self, inputs_path, config_file=None):
        """
        List content in the given path and prepare inputs for each entry.

        Parameters
        ----------
        inputs_path : str
            Path to root where input data are located.
        config_file : str
            Optional path to configuration file to associate to inputs.
        """
        inputs_path = clean_path(inputs_path)
        if inputs_path is not None:
            for input_path in glob.glob(inputs_path + '/*'):
                if os.access(inputs_path, os.R_OK):
                    self.input(input_path).make_config(config_file)

    def run(self):
        """
        Start the pipeline run.
        """
        self.dummy_job._starting_todo(logprint=False)
        self.dummy_job.child_event('__init__').fire()
        self.dummy_job._ending_todo()

    def diagnose(self):
        """
        Diagnose current state of the pipeline. TODO
        """
        pass

    def reset(self):
        """
        Reset the pipeline.
        """
        for item in self.inputs:
            item.reset()
        self.dummy_job.reset()

    def clean(self):
        """
        Fully clean the pipeline.
        """
        for item in self.nondummy_tasks:
            item.delete()
        for item in self.inputs:
            item.delete()
        remove_path(self.software_root + '/__pycache__', hard=True)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        self.clean()
        self.dummy_task.delete()
        super(Pipeline, self).delete()
        remove_path(self.software_root)
        remove_path(self.input_root)
        remove_path(self.data_root)
        remove_path(self.config_root)
        remove_path(self.pipe_root + '/.wpipe', hard=True)
