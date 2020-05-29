#!/usr/bin/env python
"""
Contains the Job class definition

Please note that this module is private. The Job class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import subprocess, datetime, si
from .core import ChildrenProxy
from .core import initialize_args, wpipe_to_sqlintf_connection
from .OptOwner import OptOwner


class Job(OptOwner):
    """
        A Job object represents the job running a specific task given a
        specific target configuration, and is defined by its task, and in most
        cases, a firing event, a configuration, and/or a node.

        Construction
        ------------

        Call signatures::

            Job(event, configuration=event.config,
                task=configuration.dummy_task, node=event.parent_job.node,
                state='any', options={})
            Job(keyid)
            Job(_job)

        When __new__ is called, it queries the database for an existing
        row in the `jobs` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Job`
        object, otherwise it creates a new row via a new `sqlintf.Job`
        instance. This `sqlintf.Job` object is then wrapped under the
        hidden attribute `Job._job` in the new instance of this `Job`
        class generated by __new__.

        All jobs are uniquely identified by their task, their configuration,
        their firing event and their node, but alternatively, the constructor
        can take as sole argument either:
         - the primary key id of the corresponding `jobs` table row
         - the `sqlintf.Job` object interfacing that table row

        After the instantiation of __new__ is completed, if a dictionary of
        options was given to the constructor, the __init__ method constructs
        a set of Option objects owned by the job.

        Parameters
        ----------
        event : Event object
            Firing Event owning this job.
        configuration : Configuration object
            Configuration owning this job - defaults to event.config.
        task : Task object
            Task owning this job - defaults to config.dummy_task.
        node : Node object
            Node owning this job - defaults to event.parent_job.node.
        state : string
            State of this job  - defaults to 'any'.
        options : dict
            Dictionary of options to associate to the job.
        keyid : int
            Primary key id of the table row.
        _job : sqlintf.Job object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : tuple(Task, Configuration, Node, Event)
            Points to a tuple of attributes self.task, self.config, self.node
            and self.firing_event.
        state : string
            State of this job.
        job_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        starttime : datetime.datetime object
            Timestamp of job starting time.
        endtime : datetime.datetime object
            Timestamp of job ending time.
        task_id : int
            Primary key id of the table row of parent task.
        task : Task object
            Task object corresponding to parent task.
        node_id : int
            Primary key id of the table row of parent node.
        node : Node object
            Node object corresponding to parent node.
        config_id : int
            Primary key id of the table row of parent configuration.
        config : Configuration object
            Configuration object corresponding to parent configuration.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        target_id : int
            Primary key id of the table row of parent target.
        target : Target object
            Target object corresponding to parent target.
        firing_event_id : int
            Primary key id of the table row of parent event.
        firing_event : Event object
            Event object corresponding to parent event.
        child_events : core.ChildrenProxy object
            List of Event objects owned by the job.
        optowner_id : int
            Points to attribute job_id.
        options : core.DictLikeChildrenProxy object
            Dictionary of Option objects owned by the target.

        How to use
        ----------
        Job objects are at the heart of Wpipe functionalities, as they handle
        the pipeline jobbing. A Job is constructed with a combination of
        parent Wpipe objects among 4 of them:
         - a Task object is always required, representing the task that the
           job is running,
         - a Node object that represents the node on which the job is running,
         - in most cases, an Event object for the firing event that started
           the job run,
         - and lastly, a Configuration object representing a target and its
           configuration which the job runs with.

        Every Pipeline objects are constructed with a default dummy job meant
        to start the pipeline. This particular job constitutes the only case
        in which a Job object comes with only a Task + Node parent combination
        as it is not launched by a particular firing event and is not meant to
        work on any target configuration (generally, these must be initialized
        by the very first task to be called). All other Job objects that are
        not such dummy job always require to construct a firing Event object,
        and in most cases, a target Configuration object.

        One can give each of these 4 objects in any order as arguments of the
        Job class constructor, or use the Job-generating object method of one
        of the parent object, providing the 3 other objects as arguments:

        >>> new_job = wp.Job(my_task, my_node, my_event, my_config)
        or
        >>> new_job = my_task.job(my_node, my_event, my_config)
        or
        >>> new_job = my_node.job(my_task, my_event, my_config)
        or
        >>> new_job = my_event.fired_job(my_task, my_node, my_config)
        or
        >>> new_job = my_config.job(my_task, my_node, my_event)

        Once the Job object is constructed, 3 methods are important for the
        pipeline run: logprint, child_event and submit,
         - Job.logprint allows the logging of a job in logfiles named after
           that job, its task and its firing event,
         - Job.child_event is the Event-generating object method of Job, which
           handles the starting of new jobs from an existing job,
         - Job.submit plainly submits the job to the system job scheduler;
           this method is automatically called when a job is fired by a firing
           event through its method Event.fire.
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._job = args[0] if len(args) == 1 else None
        if not isinstance(cls._job, si.Job):
            keyid = kwargs.get('id', cls._job)
            if isinstance(keyid, int):
                cls._job = si.session.query(si.Job).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                event = kwargs.get('event', wpargs.get('Event', None))
                config = kwargs.get('config',
                                    wpargs.get('Configuration',
                                               event.config if event is not None else
                                               None))
                task = kwargs.get('task', wpargs.get('Task',
                                                     config.dummy_task if config is not None else
                                                     None))
                from . import DefaultNode
                node = kwargs.get('node',
                                  wpargs.get('Node',
                                             event.parent_job.node if event is not None else
                                             DefaultNode))
                state = kwargs.get('state', 'any' if args[0] is None else args[0])
                # querying the database for existing row or create
                try:
                    cls._job = si.session.query(si.Job). \
                        filter_by(task_id=task.task_id)
                    if config is not None:
                        cls._job = cls._job. \
                            filter_by(config_id=config.config_id)
                    if event is not None:
                        cls._job = cls._job. \
                            filter_by(firing_event_id=event.event_id)
                    if node is not None:
                        cls._job = cls._job. \
                            filter_by(node_id=node.node_id)
                    cls._job = cls._job.one()
                except si.orm.exc.NoResultFound:
                    cls._job = si.Job(state=state)
                    task._task.jobs.append(cls._job)
                    if config is not None:
                        config._configuration.jobs.append(cls._job)
                    if event is not None:
                        event._event.fired_jobs.append(cls._job)
                    if node is not None:
                        node._node.jobs.append(cls._job)
                    cls._job.starttime = datetime.datetime.utcnow()
                    cls._job.endtime = datetime.datetime.utcnow()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Job')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_child_events_proxy'):
            self._child_events_proxy = ChildrenProxy(self._job, 'child_events', 'Event')
        if not hasattr(self, '_optowner'):
            self._optowner = self._job
        super(Job, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Job).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.task, self.config, self.node, self.firing_event

    @property
    def state(self):
        si.session.commit()
        return self._job.state

    @property
    def job_id(self):
        si.session.commit()
        return self._job.id

    @property
    def starttime(self):
        si.session.commit()
        return self._job.starttime

    @property
    def endtime(self):
        si.session.commit()
        return self._job.endtime

    @property
    def task_id(self):
        si.session.commit()
        return self._job.task_id

    @property
    def task(self):
        if hasattr(self._job.task, '_wpipe_object'):
            return self._job.task._wpipe_object
        else:
            from .Task import Task
            return Task(self._job.task)

    @property
    def node_id(self):
        si.session.commit()
        return self._job.node_id

    @property
    def node(self):
        if self._job.node is not None:
            if hasattr(self._job.node, '_wpipe_object'):
                return self._job.node._wpipe_object
            else:
                from .Node import Node
                return Node(self._job.node)

    @property
    def config_id(self):
        si.session.commit()
        return self._job.config_id

    @property
    def config(self):
        if self._job.config is not None:
            if hasattr(self._job.config, '_wpipe_object'):
                return self._job.config._wpipe_object
            else:
                from .Configuration import Configuration
                return Configuration(self._job.config)

    @property
    def pipeline_id(self):
        return self.task.pipeline_id

    @property
    def pipeline(self):
        return self.task.pipeline

    @property
    def target_id(self):
        return self.config.target_id

    @property
    def target(self):
        return self.config.target

    @property
    def firing_event_id(self):
        si.session.commit()
        return self._job.firing_event_id

    @property
    def firing_event(self):
        if self._job.firing_event is not None:
            if hasattr(self._job.firing_event, '_wpipe_object'):
                return self._job.firing_event._wpipe_object
            else:
                from .Event import Event
                return Event(self._job.firing_event)

    @property
    def child_events(self):
        return self._child_events_proxy

    def child_event(self, *args, **kwargs):
        from .Event import Event
        return Event(self, *args, **kwargs)

    def logprint(self, log_text):
        logpath = self.config.target.datapath + '/log_' + self.config.name + '/'
        logfile = self.task.name + '_j' + str(self.job_id) + '_e' + str(self.firing_event_id) + '.log'
        log = open(logpath + logfile, "a")
        log.write(log_text)
        log.close()

    def submit(self):
        my_pipe = self.task.pipeline
        executable = my_pipe.software_root + '/' + self.task.name
        # subprocess.Popen(
        #     [executable, '-p', str(self.pipeline_id), '-u', str(self.user_name), '-j', str(self.job_id)],
        #     cwd=my_pipe.pipe_root)
        # # This line will work with an SQL backbone, but NOT hdf5,
        # # as 2 tasks running on the same hdf5 file will collide!
        subprocess.run(
            [executable, '-p', str(my_pipe.pipeline_id), '-u', str(my_pipe.user_name), '-j', str(self.job_id)],
            cwd=my_pipe.pipe_root)
        # Let's send stuff to slurm
        # sql_hyak(self.task,self.job_id,self.firing_event_id)
        # Let's send stuff to pbs
        # sql_pbs(self.task,self.job_id,self.firing_event_id)
