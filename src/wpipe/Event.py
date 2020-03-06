from .core import *
from .Store import Store
from .Owner import SQLOwner
from .Job import Job, SQLJob


class Event:
    def __init__(self, name='', value='', jargs='', job=Job().new()):
        self.job_id = np.array([int(job.job_id)])
        self.jargs = np.array([str(jargs)])
        self.name = np.array([str(name)])
        self.value = np.array([str(value)])
        self.event_id = np.array([int(0)])

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.event_id
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, store=Store()):
        from . import Options
        _df = store.create('events', 'event_id', self)
        _opt = Options(options).create('event', int(_df.event_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(event_id, store=Store()):
        return store.select('events').loc[int(event_id)]

    def run_complete(event_id=0, store=Store()):
        from . import Options
        event = Event.get(int(event_id))
        job_id = int(event.job_id)
        jobOpt = Options.get('job', int(job_id))
        jobOpt['completed'] = int(jobOpt['completed']) + 1
        return store.update('options', Options(jobOpt).new('job', job_id))


class SQLEvent(SQLOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._event = args[0] if len(args) else None
        if not isinstance(cls._event, si.Event):
            id = kwargs.get('id', cls._event)
            if isinstance(id, int):
                cls._event = si.session.query(si.Event).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args = wpargs_from_args(*args)
                job = wpargs.get('Job', kwargs.get('job', None))
                name = args[0] if len(args) else kwargs.get('name', None)
                jargs = args[1] if len(args) > 1 else kwargs.get('jargs', '')
                value = args[2] if len(args) > 2 else kwargs.get('value', '')
                # querying the database for existing row or create
                try:
                    cls._event = si.session.query(si.Event). \
                        filter_by(parent_job_id=job.job_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._event = si.Event(name=name,
                                          jargs=jargs,
                                          value=value)
                    job._job.child_events.append(cls._event)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Event', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_fired_jobs_proxy'):
            self._fired_jobs_proxy = ChildrenProxy(self._event, 'fired_jobs', 'Job', __name__,
                                                   child_attr='id')
        if not hasattr(self, '_owner'):
            super().__init__()
            self._owner = self._event
        self.options = kwargs.get('options', {})
        self._event.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parents(self):
        return self.parent_job

    @property
    def name(self):
        si.session.commit()
        return self._event.name

    @name.setter
    def name(self, name):
        self._event.name = name
        self._event.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def event_id(self):
        si.session.commit()
        return self._event.id

    @property
    def jargs(self):
        si.session.commit()
        return self._event.jargs

    @property
    def value(self):
        si.session.commit()
        return self._event.value

    @property
    def parent_job_id(self):
        si.session.commit()
        return self._event.parent_job_id

    @property
    def parent_job(self):
        if hasattr(self._event.parent_job, '_wpipe_object'):
            return self._event.parent_job._wpipe_object
        else:
            from .Job import SQLJob
            return SQLJob(self._event.parent_job)

    @property
    def config(self):
        return self.parent_job.config

    @property
    def fired_jobs(self):
        return self._fired_jobs_proxy

    def fired_job(self, *args, **kwargs):
        from .Job import SQLJob
        return SQLJob(self, *args, **kwargs)
