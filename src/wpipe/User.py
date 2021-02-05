#!/usr/bin/env python
"""
Contains the User class definition

Please note that this module is private. The User class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import datetime, si
from .core import ChildrenProxy
from .core import initialize_args, wpipe_to_sqlintf_connection, in_session
from .core import split_path
from .core import PARSER

__all__ = ['User']


def _in_session(**local_kw):
    return in_session('_%s' %  split_path(__file__)[1].lower(), **local_kw)


class User:
    """
        Represents a wingspipe user.

        Call signatures::

            User(name=PARSER.user_name)
            User(keyid)
            User(_user)

        When __new__ is called, it queries the database for an existing
        row in the `users` table via `sqlintf` using the given signature.
        If the row exists, it retrieves its corresponding `sqlintf.User`
        object, otherwise it creates a new row via a new `sqlintf.User`
        instance. This `sqlintf.User` object is then wrapped under the
        hidden attribute `User._user` in the new instance of this `User`
        class generated by __new__.

        All users are uniquely identified by their name, but alternatively,
        the constructor can take as sole argument either:
         - the primary key id of the corresponding `users` table row
         - the `sqlintf.User` object interfacing that table row

        Parameters
        ----------
        name : string
            Name of user - defaults to PARSER.user_name (see Notes).
        keyid : int
            Primary key id of the table row.
        _user : sqlintf.User object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : None
            Dummy attribute with None value.
        name : string
            Name of user.
        user_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        pipelines : core.ChildrenProxy object
            List of Pipeline objects owned by user.

        See Also
        --------
        DefaultUser : User constructed at wpipe importation.

        Notes
        -----
        Importing wpipe constructs a default user stored in DefaultUser. This
        makes use of the PARSER.user_name string which generally corresponds
        to the user named 'default' but can be configured:
        - either, evidently, via a parse argument -u/--user
        - or via a pre-defined environment variable WPIPE_USER (recommended)
    """
    __cache__ = {}

    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._user = args[0] if len(args) else None
        if not isinstance(cls._user, si.User):
            keyid = kwargs.get('id', cls._user)
            if isinstance(keyid, int):
                with si.begin_session() as session:
                    cls._user = session.query(si.User).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                name = kwargs.get('name', PARSER.parse_known_args()[0].user_name if args[0] is None else args[0])
                # querying the database for existing row or create
                with si.begin_session() as session:
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._user = this_nested.session.query(si.User).with_for_update(). \
                                filter_by(name=name).one_or_none()
                            if cls._user is None:
                                cls._user = si.User(name=name)
                                this_nested.session.add(cls._user)
                                this_nested.commit()
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'User')
        return cls._inst

    @_in_session()
    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_pipelines_proxy'):
            self._pipelines_proxy = ChildrenProxy(self._user, 'pipelines', 'Pipeline', child_attr='pipe_root')
        self._user.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @classmethod
    def select(cls, **kwargs):
        """
        Returns a list of User objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.User` for parameters.

        Returns
        -------
        out : list of User object
            list of objects fulfilling the kwargs filter.
        """
        with si.begin_session() as session:
            cls._temp = session.query(si.User).filter_by(**kwargs)
            return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        None: Dummy attribute with None value.
        """
        return

    @property
    @_in_session()
    def name(self):
        """
        str: Name of user.
        """
        self._session.refresh(self._user)
        return self._user.name

    @name.setter
    @_in_session()
    def name(self, name):
        # with si.begin_session() as session:
        #     # session.add(self._user)
        #     self._user.name = name
        #     self._user.timestamp = datetime.datetime.utcnow()
        #     session.commit()
        self._user.name = name
        self._user.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
    def user_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._user.id

    @property
    @_in_session()
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        self._session.refresh(self._user)
        return self._user.timestamp

    @property
    def pipelines(self):
        """
        :obj:`core.ChildrenProxy`: List of Pipeline objects owned by user.
        """
        return self._pipelines_proxy

    def pipeline(self, *args, **kwargs):
        """
        Returns a pipeline owned by the user.

        Parameters
        ----------
        kwargs
            Refer to :class:`Pipeline` for parameters.

        Returns
        -------
        pipeline : :obj:`Pipeline`
            Pipeline corresponding to given kwargs.
        """
        from .Pipeline import Pipeline
        return Pipeline(self, *args, **kwargs)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        self.pipelines.delete()
        si.delete(self._user)
