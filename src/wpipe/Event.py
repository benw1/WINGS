from .core import *
from .Owner import SQLOwner


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
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=4)
                job = kwargs.get('job', wpargs.get('Job', None))
                name = kwargs.get('name', args[0])
                tag = kwargs.get('tag', '' if args[1] is None else args[1])
                jargs = kwargs.get('jargs', '' if args[2] is None else args[2])
                value = kwargs.get('value', '' if args[3] is None else args[3])
                # querying the database for existing row or create
                try:
                    cls._event = si.session.query(si.Event). \
                        filter_by(parent_job_id=job.job_id). \
                        filter_by(name=name). \
                        filter_by(tag=tag).one()
                except si.orm.exc.NoResultFound:
                    cls._event = si.Event(name=name,
                                          tag=tag,
                                          jargs=jargs,
                                          value=value)
                    job._job.child_events.append(cls._event)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Event')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_fired_jobs_proxy'):
            self._fired_jobs_proxy = ChildrenProxy(self._event, 'fired_jobs', 'Job',
                                                   child_attr='id')
        if not hasattr(self, '_owner'):
            self._owner = self._event
        super(SQLEvent, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Event).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

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
    def tag(self):
        si.session.commit()
        return self._event.name

    @tag.setter
    def tag(self, tag):
        self._event.tag = tag
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
    def pipeline(self):
        return self.parent_job.pipeline

    @property
    def fired_jobs(self):
        return self._fired_jobs_proxy

    def fired_job(self, *args, **kwargs):
        from .Job import SQLJob
        return SQLJob(self, *args, **kwargs)

    def fire(self):
        # print("HERE ",self.name," DONE")
        try:
            from .Configuration import SQLConfiguration
            configuration = SQLConfiguration(self.options['config_id'])
        except KeyError:
            configuration = self.config
        # print(self.parent_job.pipeline_id)
        for task in self.pipeline.tasks:
            # print(task.task_id)
            for mask in task.masks:
                # print("HERE",self.name,mask.name,self.value,mask.value,"DONE3")
                if (self.name == mask.name) & ((self.value == mask.value) | (mask.value == '*')):
                    new_job = self.fired_job(task, configuration)
                    print(task.name, "-j", new_job.job_id)
                    new_job.submit()  # pipeline should be able to run stuff and keep track if it completes
                    return
