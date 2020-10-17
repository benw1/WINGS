#!/usr/bin/env python
"""
Contains the Event class definition

Please note that this module is private. The Event class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, subprocess, si
from .core import ChildrenProxy
from .core import initialize_args, wpipe_to_sqlintf_connection, as_int
from .core import PARSER
from .OptOwner import OptOwner

__all__ = ['Event']


class Event(OptOwner):
    """
        Represents a fired event of a WINGS pipeline.

        Call signatures::

            Event(job, name, tag='', jargs='', value='', options={})
            Event(keyid=PARSER.event_id)
            Event(_event)

        When __new__ is called, it queries the database for an existing
        row in the `events` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.Event`
        object, otherwise it creates a new row via a new `sqlintf.Event`
        instance. This `sqlintf.Event` object is then wrapped under the
        hidden attribute `Event._event` in the new instance of this `Event`
        class generated by __new__.

        All events are uniquely identified by their parent job, their name,
        and their tag, but alternatively, the constructor can take as sole
        argument either:
         - the primary key id of the corresponding `events` table row
         - the `sqlintf.Event` object interfacing that table row
        It can also be called without argument if the python script importing
        wpipe was ran with the command-line argument --event/-e referring to a
        valid event primary key id, in which case the consequently constructed
        event will correspond to that event.

        After the instantiation of __new__ is completed, if a dictionary of
        options was given to the constructor, the __init__ method constructs
        a set of Option objects owned by the event.

        Parameters
        ----------
        job : Job object
            Parent Job owning this event.
        name : string
            Name of the mask which task is meant to be the fired job task.
        tag : string
            Unique tag to identify the event if multiple event instance fire
            the same task - defaults to ''.
        jargs : string
            ###BEN### - defaults to ''.
        value : string
            Value of the mask which task is meant to be the fired job task
            - defaults to ''.
        options : dict
            Dictionary of options to associate to the event.
        keyid : int
            Primary key id of the table row.
        _event : sqlintf.Event object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Job object
            Points to attribute self.parent_job.
        name : string
            Name of the mask which task is meant to be the fired job task.
        tag : string
            Unique tag to identify the event if multiple event instance fire
            the same task.
        event_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        jargs : string
            ###BEN###
        value : string
            Value of the mask which task is meant to be the fired job task.
        parent_job_id : int
            Primary key id of the table row of parent job.
        parent_job : Job object
            Job object corresponding to parent job.
        config : Configuration object
            Configuration object corresponding to parent configuration.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        fired_jobs : core.ChildrenProxy object
            List of Job objects owned by the event.
        optowner_id : int
            Points to attribute event_id.
        options : core.DictLikeChildrenProxy object
            Dictionary of Option objects owned by the event.

        Notes
        -----
        An Event object requires a Job object to construct: this can be
        achieved either by using the object method Job.child_event, or
        alternatively by using the Event class constructor giving it the Job
        object as argument. In both cases, the signature must also contain the
        name of the mask which task is meant to be the one of the
        subsequently-fired job:

        >>> my_event = my_job.child_event(name_of_task_mask)
        or
        >>> my_event = wp.Event(my_job, name_of_task_mask)

        An event is meant to fire a new job, and accordingly the Event object
        comes with an object method Event.fire. This method searches for the
        task which masks correspond to the name and value signature, and then
        fire a new job owned by this task. The new job is also owned to the
        parent configuration of the parent job of the event, unless the event
        owns an option named 'config_id' in which case the job is owned by the
        corresponding configuration.
    """

    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._event = args[0] if len(args) else as_int(PARSER.parse_known_args()[0].event_id)
        if not isinstance(cls._event, si.Event):
            keyid = kwargs.get('id', cls._event)
            if isinstance(keyid, int):
                cls._event = si.session.query(si.Event).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=4)
                job = kwargs.get('job', wpargs.get('Job', None))
                name = kwargs.get('name', args[0])
                tag = kwargs.get('tag', '' if args[1] is None else args[1])
                jargs = kwargs.get('jargs', '' if args[2] is None else args[2])
                value = kwargs.get('value', '' if args[3] is None else args[3])
                # querying the database for existing row or create
                for retry in si.retrying_nested():
                    with retry:
                        this_nested = si.begin_nested()
                        try:
                            cls._event = si.session.query(si.Event).with_for_update(). \
                                filter_by(parent_job_id=job.job_id). \
                                filter_by(name=name). \
                                filter_by(tag=tag).one()
                            this_nested.rollback()
                        except si.orm.exc.NoResultFound:
                            cls._event = si.Event(name=name,
                                                  tag=tag,
                                                  jargs=jargs,
                                                  value=value)
                            job._job.child_events.append(cls._event)
                            this_nested.commit()
                        retry.retry_state.commit()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Event')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_fired_jobs_proxy'):
            self._fired_jobs_proxy = ChildrenProxy(self._event, 'fired_jobs', 'Job',
                                                   child_attr='id')
        if not hasattr(self, '_optowner'):
            self._optowner = self._event
        super(Event, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of Event objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.Event` for parameters.

        Returns
        -------
        out : list of Event object
            list of objects fulfilling the kwargs filter.
        """
        cls._temp = si.session.query(si.Event).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Job`: Points to attribute self.parent_job.
        """
        return self.parent_job

    @property
    def name(self):
        """
        str: Name of the mask which task is meant to be the fired job task.
        """
        si.commit()
        return self._event.name

    @name.setter
    def name(self, name):
        self._event.name = name
        self._event.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def tag(self):
        """
        str: Unique tag to identify the event if multiple event instance fire
        the same task.
        """
        si.commit()
        return self._event.tag

    @tag.setter
    def tag(self, tag):
        self._event.tag = tag
        self._event.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def event_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._event.id

    @property
    def jargs(self):
        """
        str: ###BEN###
        """
        return self._event.jargs

    @property
    def value(self):
        """
        str: Value of the mask which task is meant to be the fired job task.
        """
        si.commit()
        return self._event.value

    @value.setter
    def value(self, value):
        self._event.value = value
        self._event.timestamp = datetime.datetime.utcnow()
        si.commit()

    @property
    def parent_job_id(self):
        """
        int: Primary key id of the table row of parent job.
        """
        return self._event.parent_job_id

    @property
    def parent_job(self):
        """
        :obj:`Job`: Job object corresponding to parent job.
        """
        if hasattr(self._event.parent_job, '_wpipe_object'):
            return self._event.parent_job._wpipe_object
        else:
            from .Job import Job
            return Job(self._event.parent_job)

    @property
    def config(self):
        """
        :obj:`Configuration`: Configuration object corresponding to parent
        configuration.
        """
        try:
            from . import Configuration
            return Configuration(self.options['config_id'])
        except KeyError:
            return self.parent_job.config

    @property
    def pipeline(self):
        """
        :obj:`Pipeline`: Pipeline object corresponding to parent pipeline.
        """
        return self.parent_job.pipeline

    @property
    def fired_jobs(self):
        """
        :obj:`core.ChildrenProxy`: List of Job objects owned by the event.
        """
        return self._fired_jobs_proxy

    def fired_job(self, *args, **kwargs):
        """
        Returns a job owned by the event.

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

    def fire(self):
        """
        Fire the task associated to this event.

        Notes
        -----
        If this task was already previously fired, the method check if its
        job has completed. In the case it did, it calls the fire method of
        each child event of that jobs. In the case, it did not complete, it
        either fires the task again if the job did not complete due to an
        error, or it does nothing if the job is just still running.
        """
        if len(self.fired_jobs):
            fired_job = self.fired_jobs[-1]
            if fired_job.has_completed:
                if len(fired_job.child_events):
                    for child_event in fired_job.child_events:
                        child_event.fire()
                else:
                    print()  # that branch has completed
            else:
                if fired_job.is_active:
                    print()  # fired_job keep going
                else:
                    if fired_job.task_changed:
                        self.__fire(fired_job.task)
                    else:
                        print()  # task will produce same error
        else:
            for task in self.pipeline.tasks:
                for mask in task.masks:
                    if (self.name == mask.name) & ((self.value == mask.value) | (mask.value == '*')):
                        self.__fire(task)
                        return
            raise ValueError(
                "No mask corresponding to event signature {name='%s',value='%s'}" % (self.name, self.value))

    def __fire(self, task):  # MEH
        my_pipe = self.pipeline
        with my_pipe.dummy_job.logprint().open("a") as stdouterr:
            options = self.options

            print(options)
            submission_type = None

            # TODO: Clean this mess up
            if self.config is not None:
                try:
                    configParameters = self.config.parameters  # dict
                    submission_type = configParameters['submission_type']
                except KeyError:
                    pass
            try:
                submission_type = options['submission_type']
            except KeyError:
                pass
            if submission_type is None:
                print(task.executable, '-e', str(self.event_id), ''+si.core.verbose*'-v')
                subprocess.Popen([task.executable, '-e', str(self.event_id)]+si.core.verbose*['-v'],
                                 cwd=my_pipe.pipe_root, stdout=stdouterr, stderr=stdouterr)
            elif 'pbs' == submission_type:
                from . import sendJobToPbs
                sendJobToPbs(self._generate_new_job(task))
                return
            elif 'hyak' == submission_type:
                pass
            else:
                raise ValueError("'%s' isn't a valid 'submission_type'" % submission_type)

    def _generate_new_job(self, task):
        return self.fired_job(len(self.fired_jobs) + 1, task, self.config)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        for item in self.fired_jobs:
            item.delete()
        super(Event, self).delete()
