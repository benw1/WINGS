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
        job = args[0]
        name = args[1]
        jargs = kwargs.get('jargs','')
        value = kwargs.get('value','')
        try:
            cls._event = si.session.query(si.Event). \
                filter_by(parent_job_id=job.job_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            cls._event = si.Event(name=name,
                                  jargs=jargs,
                                  value=value)
            job._job.child_events.append(cls._event)
        if hasattr(cls._event, '_wpipe_object'):
            cls._inst = cls._event._wpipe_object
        else:
            cls._inst = super(SQLEvent, cls).__new__(cls)
            cls._inst._event = cls._event
            cls._event._wpipe_object = cls._inst
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self,'_owner'):
            super().__init__()
            self._owner = self._event
        self.options = kwargs.get('options',{})
        self._event.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
    def timestamp(self):
        si.session.commit()
        return self._event.timestamp

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
    def fired_jobs_ids(self):
        si.session.commit()
        return list(map(lambda job: job.id, self._event.fired_jobs))
