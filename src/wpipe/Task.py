#!/usr/bin/env python
"""
Contains the Task class definition

Please note that this module is private. The Task class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import os, shutil, warnings, datetime, si
from .core import ChildrenProxy
from .core import initialize_args, wpipe_to_sqlintf_connection, clean_path

__all__ = ['Task']


class Task:
    """
        Represents a pipeline's task.

        Call signatures::

            Task(pipeline, path, nruns=0, run_time=0, is_exclusive=0)
            Task(keyid)
            Task(_task)

        When __new__ is called, it queries the database for an existing
        row in the `tasks` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Task`
        object, otherwise it creates a new row via a new `sqlintf.Task`
        instance. This `sqlintf.Task` object is then wrapped under the
        hidden attribute `Task._task` in the new instance of this `Task`
        class generated by __new__.

        In the latter case where a new row is created, the script gets built
        as part of the pipeline software suite by copying to the pipeline
        software_root the file located in the path given to the constructor.

        All tasks are uniquely identified by their pipeline and their name,
        which latter is determined by __new__ as the tail returned by
        os.path.split(path) of the path parameter, but alternatively, the
        constructor can take as sole argument either:
         - the primary key id of the corresponding `tasks` table row
         - the `sqlintf.Task` object interfacing that table row

        Parameters
        ----------
        pipeline : Pipeline object
            Pipeline owning this task.
        path : string
            Path to the task script file.
        nruns : int
            ###BEN### - defaults to 0.
        run_time : int
            ###BEN### - defaults to 0.
        is_exclusive : int
            ###BEN### - defaults to 0.
        keyid : int
            Primary key id of the table row.
        _task : sqlintf.Task object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Pipeline object
            Points to attribute self.pipeline.
        name : string
            Name of the task.
        task_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        nruns : int
            ###BEN###
        run_time : int
            ###BEN###
        is_exclusive : int
            ###BEN###
        executable : str
            Path where the script the task points to is located.
        last_modification_timestamp : datetime.datetime object
            Timestamp of last modification time of the task script.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        masks : core.ChildrenProxy object
            List of Mask objects owned by the task.
        jobs : core.ChildrenProxy object
            List of Job objects owned by the task.

        Notes
        -----
        A Task object requires a Pipeline object to construct: it is notably
        recommended to use the method attach_tasks of that Pipeline object
        for constructing a collection of tasks associated to the pipeline.
        Nevertheless, one can manually construct a Task object calling the
        constructor with the parent Pipeline object and the path where to find
        the script in arguments.

        >>> my_task = wp.Task(my_pipe, path_to_script)

        A task can be run as a job: accordingly, the Task object can generate
        a Job object associated to it using the object method job. This method
        has the same call signature as the Job object constructor without
        specifying the task parameter: that is basically by providing the
        other optional parameters such as its parent Configuration, Event and
        Node objects if needed.

        >>> new_job = my_task.job(my_node, my_event, my_config)
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._task = args[0] if len(args) else None
        if not isinstance(cls._task, si.Task):
            keyid = kwargs.get('id', cls._task)
            if isinstance(keyid, int):
                cls._task = si.session.query(si.Task).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=4)
                pipeline = kwargs.get('pipeline', wpargs.get('Pipeline', None))
                base, name = os.path.split(clean_path(kwargs.get('path', args[0])))
                nruns = kwargs.get('nruns', 0 if args[1] is None else args[1])
                run_time = kwargs.get('run_time', 0 if args[2] is None else args[2])
                is_exclusive = kwargs.get('is_exclusive', 0 if args[3] is None else args[3])
                # querying the database for existing row or create
                try:
                    cls._task = si.session.query(si.Task). \
                        filter_by(pipeline_id=pipeline.pipeline_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._task = si.Task(name=name,
                                        nruns=nruns,
                                        run_time=run_time,
                                        is_exclusive=is_exclusive)
                    pipeline._pipeline.tasks.append(cls._task)
                    if base != pipeline.software_root:
                        shutil.copy2(base+'/'+name, pipeline.software_root+'/')
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Task')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_masks_proxy'):
            self._masks_proxy = ChildrenProxy(self._task, 'masks', 'Mask')
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._task, 'jobs', 'Job',
                                             child_attr='id')
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Task objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Task` for parameters.

        Returns
        -------
        out : list of Task object
            list of objects fulfilling the kwargs filter.
        """
        cls._temp = si.session.query(si.Task).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Pipeline`: Points to attribute self.pipeline.
        """
        return self.pipeline

    @property
    def name(self):
        """
        str: Name of the task.
        """
        si.session.commit()
        return self._task.name

    @name.setter
    def name(self, name):
        self._task.name = name
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def task_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._task.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        si.session.commit()
        return self._task.timestamp

    @property
    def nruns(self):
        """
        int: ###BEN###
        """
        si.session.commit()
        return self._task.nruns

    @property
    def run_time(self):
        """
        int: ###BEN###
        """
        si.session.commit()
        return self._task.run_time

    @property
    def is_exclusive(self):
        """
        int: ###BEN###
        """
        si.session.commit()
        return self._task.is_exclusive

    @property
    def executable(self):
        """
        str: Path where the script the task points to is located.
        """
        return self.pipeline.software_root + '/' + self.name

    @property
    def last_modification_timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last modification time of the
        task script.
        """
        return datetime.datetime.fromtimestamp(os.path.getmtime(self.executable))

    @property
    def pipeline_id(self):
        """
        int: Primary key id of the table row of parent pipeline.
        """
        return self._task.pipeline_id

    @property
    def pipeline(self):
        """
        :obj:`Pipeline`: Pipeline object corresponding to parent pipeline.
        """
        if hasattr(self._task.pipeline, '_wpipe_object'):
            return self._task.pipeline._wpipe_object
        else:
            from .Pipeline import Pipeline
            return Pipeline(self._task.pipeline)

    @property
    def masks(self):
        """
        :obj:`core.ChildrenProxy`: List of Mask objects owned by the task.
        """
        return self._masks_proxy

    @property
    def jobs(self):
        """
        :obj:`core.ChildrenProxy`: List of Job objects owned by the task.
        """
        return self._jobs_proxy

    def mask(self, *args, **kwargs):
        """
        Returns a mask owned by the task.

        Parameters
        ----------
        kwargs
            Refer to :class:`Mask` for parameters.

        Returns
        -------
        mask : :obj:`Mask`
            Mask corresponding to given kwargs.
        """
        from .Mask import Mask
        return Mask(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        """
        Returns a job owned by the task.

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

    def register(self):
        """
        Import and call the function register implemented in task script.
        """
        _temp = __import__(os.path.basename(self.pipeline.software_root) + '.' + self.name.replace('.py', ''),
                           fromlist=[''])
        if hasattr(_temp, 'register'):
            _temp.register(self)
        else:
            warnings.warn("Task " + self.pipeline.software_root + '/' + self.name +
                          " cannot be registered: no 'register' function")
