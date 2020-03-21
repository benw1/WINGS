from .core import *
from .Store import Store
from .Node import Node
from .Owner import SQLOwner
from .Configuration import Configuration
from .Task import Task


class Job:
    def __init__(self, state='any', event_id=0,
                 task=Task().new(),
                 config=Configuration().new(),
                 node=Node().new()):
        self.state = np.array([str(state)])
        self.job_id = np.array([int(0)])
        self.event_id = np.array([int(event_id)])
        self.task_id = np.array([int(task.task_id)])
        self.config_id = np.array([int(config.config_id)])
        self.node_id = np.array([int(node.node_id)])
        self.pipeline_id = np.array([int(config.pipeline_id)])
        self.starttime = pd.to_datetime(time.time(), unit='s')
        self.endtime = pd.to_datetime(time.time(), unit='s')
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = update_time((pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                                   np.array([int(self.task_id)]),
                                                                   np.array([int(self.config_id)]),
                                                                   np.array([int(self.event_id)]),
                                                                   np.array([int(self.job_id)])],
                                                           names=('pipelineID', 'taskID', 'configID',
                                                                  'eventID', 'jobID'))))
        _df.endtime = _df.timestamp.copy()
        return _df

    def create(self, options={'completed': 0}, ret_opt=False, store=Store()):
        from . import Options
        _df = store.create('jobs', 'job_id', self)
        _opt = Options(options).create('job', int(_df.job_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(job_id, store=Store()):
        x = store.select('jobs', 'job_id==' + str(job_id))
        return x.loc[x.index.values[0]]

    def getEvent(job,
                 name='any', value='0', jargs='0',
                 options={'any': 0}, store=Store()):
        from . import Event
        return Event(name, value, jargs, job).create(options=options, store=store)


class SQLJob(SQLOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._job = args[0] if len(args) == 1 else None
        if not isinstance(cls._job, si.Job):
            id = kwargs.get('id', cls._job)
            if isinstance(id, int):
                cls._job = si.session.query(si.Job).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                event = kwargs.get('event', wpargs.get('Event', None))
                config = kwargs.get('config',
                                    wpargs.get('Configuration',
                                               event.config if event is not None else
                                               None))
                task = kwargs.get('task', wpargs.get('Task', config.dummy_task))
                from . import DefaultNode
                node = kwargs.get('node',
                                  wpargs.get('Node',
                                             event.parent_job.node if event is not None else
                                             DefaultNode))
                state = kwargs.get('state', 'any' if args[0] is None else args[0])
                # querying the database for existing row or create
                try:
                    cls._job = si.session.query(si.Job). \
                        filter_by(config_id=config.config_id). \
                        filter_by(task_id=task.task_id)
                    if event is not None:
                        cls._job = cls._job. \
                            filter_by(firing_event_id=event.event_id)
                    if node is not None:
                        cls._job = cls._job. \
                            filter_by(node_id=node.node_id)
                    cls._job = cls._job.one()
                except si.orm.exc.NoResultFound:
                    cls._job = si.Job(state=state)
                    config._configuration.jobs.append(cls._job)
                    if task is not None:
                        task._task.jobs.append(cls._job)
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
        super(SQLJob, self).__init__(kwargs.get('options', {}))

    @property
    def parents(self):
        return self.config, self.task, self.node, self.firing_event

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
            from .Task import SQLTask
            return SQLTask(self._job.task)

    @property
    def node_id(self):
        si.session.commit()
        return self._job.node_id

    @property
    def node(self):
        if hasattr(self._job.node, '_wpipe_object'):
            return self._job.node._wpipe_object
        else:
            from .Node import SQLNode
            return SQLNode(self._job.node)

    @property
    def config_id(self):
        si.session.commit()
        return self._job.config_id

    @property
    def config(self):
        if hasattr(self._job.config, '_wpipe_object'):
            return self._job.config._wpipe_object
        else:
            from .Configuration import SQLConfiguration
            return SQLConfiguration(self._job.config)

    @property
    def pipeline_id(self):
        return self.config.pipeline_id

    @property
    def pipeline(self):
        return self.config.pipeline

    @property
    def firing_event_id(self):
        si.session.commit()
        return self._job.firing_event_id

    @property
    def firing_event(self):
        if self._job.firing_event is None:
            return None
        else:
            if hasattr(self._job.firing_event, '_wpipe_object'):
                return self._job.firing_event._wpipe_object
            else:
                from .Event import SQLEvent
                return SQLEvent(self._job.firing_event)

    @property
    def child_events(self):
        return self._child_events_proxy

    def child_event(self, *args, **kwargs):
        from .Event import SQLEvent
        return SQLEvent(self, *args, **kwargs)

    def logprint(self, log_text):
        logpath = self.config.target.datapath + '/log_' + self.config.name + '/'
        logfile = self.task.name + '_j' + str(self.job_id) + '_e' + str(self.firing_event_id) + '.log'
        try:
            log = open(logpath + logfile, "a")
        except:
            log = open(logpath + logfile, "w")
        log.write(log_text)
        log.close()

    def submit(self):
        return None
