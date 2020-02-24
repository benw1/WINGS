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
    def __init__(self, job, name, jargs='', value='',
                 options={}):
        super().__init__()
        try:
            self._event = si.session.query(si.Event). \
                filter_by(job_id=job.job_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._event = si.Event(name=name,
                                   jargs=jargs,
                                   value=value)
            job._job.event = self._event
        self._owner = self._event
        self.options = options
        self._event.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._event.name

    @name.setter
    def name(self, name):
        self._event.name = name
        self._event.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def event_id(self):
        return self._event.id

    @property
    def timestamp(self):
        return self._event.timestamp

    @property
    def jargs(self):
        return self._event.jargs

    @property
    def value(self):
        return self._event.value

    @property
    def job_id(self):
        return self._event.job_id
