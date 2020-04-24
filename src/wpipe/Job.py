from .core import *
from .Owner import Owner


class Job(Owner):
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
        if not hasattr(self, '_owner'):
            self._owner = self._job
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
