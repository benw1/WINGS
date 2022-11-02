#!/usr/bin/env python
"""
Contains the DataProduct class definition

Please note that this module is private. The DataProduct class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import os, shutil, datetime, pd, si
from .core import make_yield_session_if_not_cached, make_query_rtn_upd
from .core import initialize_args, wpipe_to_sqlintf_connection, in_session, return_dict_of_attrs
from .core import clean_path, remove_path, split_path
from .OptOwner import OptOwner

__all__ = ['DataProduct']

CLASS_NAME = split_path(__file__)[1]
KEYID_ATTR = 'dp_id'
UNIQ_ATTRS = getattr(si, CLASS_NAME).__UNIQ_ATTRS__
CLASS_LOW = CLASS_NAME.lower()


def _in_session(**local_kw):
    return in_session('_%s' % CLASS_LOW, **local_kw)


_check_in_cache = make_yield_session_if_not_cached(KEYID_ATTR, UNIQ_ATTRS, CLASS_LOW)

_query_return_and_update_cached_row = make_query_rtn_upd(CLASS_LOW, KEYID_ATTR, UNIQ_ATTRS)


class DataProduct(OptOwner):
    """
        Represents a dataproduct owned by a pipeline, an input or a config.

        Call signatures::

            DataProduct(dpowner, filename, relativepath, group, data_type='',
                        subtype='', filtername='', ra=0, dec=0,
                        pointing_angle=0, options={})
            DataProduct(keyid)
            DataProduct(_dataproduct)

        When __new__ is called, it queries the database for an existing
        row in the `dataproducts` table via `sqlintf` using the given
        signature. If the row exists, it retrieves its corresponding
        `sqlintf.DataProduct` object, otherwise it creates a new row via a new
        `sqlintf.DataProduct` instance. This `sqlintf.DataProduct` object is
        then wrapped under the hidden attribute `DataProduct._dataproduct` in
        the new instance of this `DataProduct` class generated by __new__.

        All dataproducts are uniquely identified by their parent dpowner
        (pipeline, input or configuration), their filename, and their group,
        but alternatively, the constructor can take as sole argument either:
         - the primary key id of the corresponding `dataproducts` table row
         - the `sqlintf.DataProduct` object interfacing that table row

        After the instantiation of __new__ is completed, if a dictionary of
        options was given to the constructor, the __init__ method constructs
        a set of Option objects owned by the dataproduct.

        Parameters
        ----------
        dpowner : Pipeline, Input or Configuration object
            Parent Input or Configuration owning this dataproduct.
        filename : string
            Name of the file the dataproduct points to.
        relativepath : string
            Path of the directory in which the file the dataproduct points to
            is located.
        group : string
            Group of the dataproduct ('raw', 'conf', 'log' or 'proc').
        data_type : string
            Type of the data - defaults to ''.
        subtype : string
            Subtype of the data - defaults to ''.
        filtername : string
            Name of the filter of the data - defaults to ''.
        ra : int
            Right ascension coordinate of the data - defaults to 0.
        dec : int
            Declination coordinate of the data - defaults to 0.
        pointing_angle : int
            Pointing angle coordinate of the data - defaults to 0.
        options : dict
            Dictionary of options to associate to the dataproduct.
        keyid : int
            Primary key id of the table row.
        _dataproduct : sqlintf.DataProduct object exposing SQL interface
            Corresponding sqlintf object interfacing the table row.

        Attributes
        ----------
        parents : Pipeline, Input or Configuration object
            Points to attribute self.dpowner.
        filename : string
            Name of the file the dataproduct points to.
        dp_id : int
            Primary key id of the table row.
        timestamp : datetime.datetime object
            Timestamp of last access to table row.
        relativepath : string
            Path of the directory in which the file the dataproduct points to
            is located.
        path : string
            Path where the file the dataproduct points to is located.
        suffix : string
            Extension of the file the dataproduct points to.
        data_type : string
            Type of the data.
        subtype : string
            Subtype of the data.
        group : string
            Group of the dataproduct ('raw', 'conf', 'log' or 'proc').
        filtername : string
            Name of the filter of the data.
        ra : int
            Right ascension coordinate of the data.
        dec : int
            Declination coordinate of the data.
        pointing_angle : int
            Pointing angle coordinate of the data.
        dpowner_id : int
            Primary key id of the table row of parent pipeline, input or
            configuration.
        config_id : int
            Primary key id of the table row of parent configuration - raise an
            AttributeError if the parent is not a Configuration object.
        input_id : int
            Primary key id of the table row of parent input - raise an
            AttributeError if the parent is not an Input object.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        dpowner : Pipeline, Input or Configuration object
            Pipeline, Input or Configuration object corresponding to parent
            pipeline, input or configuration.
        config : Configuration object
            Configuration object corresponding to parent configuration - raise
            an AttributeError if the parent is not a Configuration object.
        input : Input object
            Input object corresponding to parent input - raise an
            AttributeError if the parent is not an Input object.
        pipeline : Pipeline object
            Pipeline object corresponding to parent pipeline.
        target : Target object
            Target object corresponding to parent target - raise
            an AttributeError if the owner is not a Configuration object.
        target_id : int
            Primary key id of the table row of parent target - raise an
            AttributeError if the owner is not a Configuration object.
        optowner_id : int
            Points to attribute dp_id.
        options : core.DictLikeChildrenProxy object
            Dictionary of Option objects owned by the target.

        Notes
        -----
        A DataProduct object constructs from dataproduct owner, that can be
        either a Pipeline, Input object or a Configuration object: this can be
        achieved either by using the dataproduct generating object method of
        such dataproduct owner object, or alternatively by using the
        DataProduct class constructor giving it the dataproduct owner object
        (Pipeline, Input or Configuration) as argument. In both cases, the
        signature must also contain the filename of the data file as well as
        its group:

        >>> my_dp = my_pipe.dataproduct(filename, group)
        or
        >>> my_dp = my_input.dataproduct(filename, group)
        or
        >>> my_dp = my_config.dataproduct(filename, group)
        or
        >>> my_dp = wp.DataProduct(my_pipe, filename, group)
        or
        >>> my_dp = wp.DataProduct(my_input, filename, group)
        or
        >>> my_dp = wp.DataProduct(my_config, filename, group)
    """
    __cache__ = pd.DataFrame(columns=[KEYID_ATTR]+UNIQ_ATTRS+[CLASS_LOW])

    @classmethod
    def _check_in_cache(cls, kind, loc):
        return _check_in_cache(cls, kind, loc)

    @classmethod
    def _sqlintf_instance_argument(cls):
        if hasattr(cls, '_%s' % CLASS_LOW):
            for _session in cls._check_in_cache(kind='keyid',
                                                loc=getattr(cls, '_%s' % CLASS_LOW).get_id()):
                pass
    
    @classmethod
    def _return_cached_instances(cls):
        return [getattr(obj, '_%s' % CLASS_LOW) for obj in cls.__cache__[CLASS_LOW]]

    def __new__(cls, *args, **kwargs):
        if hasattr(cls, '_inst'):
            old_cls_inst = cls._inst
            delattr(cls, '_inst')
        else:
            old_cls_inst = None
        cls._to_cache = {}
        # checking if given argument is sqlintf object or existing id
        cls._dataproduct = args[0] if len(args) else None
        if not isinstance(cls._dataproduct, si.DataProduct):
            keyid = kwargs.get('id', cls._dataproduct)
            if isinstance(keyid, int):
                for session in cls._check_in_cache(kind='keyid', loc=keyid):
                    cls._dataproduct = session.query(si.DataProduct).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=9)
                list(wpargs.__setitem__('DPOwner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.DPOwner.__subclasses__())))
                dpowner = kwargs.get('dpowner', wpargs.get('DPOwner', None))
                filename = kwargs.get('filename', args[0])
                relativepath = clean_path(kwargs.get('relativepath', args[1]))
                group = kwargs.get('group', args[2])  # TODO: Switch group and relativepath args order
                data_type = kwargs.get('data_type', '' if args[3] is None else args[3])
                subtype = kwargs.get('subtype', '' if args[4] is None else args[4])
                filtername = kwargs.get('filtername', '' if args[5] is None else args[5])
                ra = kwargs.get('ra', 0 if args[6] is None else args[6])
                dec = kwargs.get('dec', 0 if args[7] is None else args[7])
                pointing_angle = kwargs.get('pointing_angle', 0 if args[8] is None else args[8])
                # querying the database for existing row or create
                for session in cls._check_in_cache(kind='args', loc=(dpowner.dpowner_id, group, filename)):
                    for retry in session.retrying_nested():
                        with retry:
                            this_nested = retry.retry_state.begin_nested()
                            cls._dataproduct = this_nested.session.query(si.DataProduct).with_for_update(). \
                                filter_by(dpowner_id=dpowner.dpowner_id). \
                                filter_by(group=group). \
                                filter_by(filename=filename).one_or_none()
                            if cls._dataproduct is None:
                                if '.' in filename:
                                    _suffix = filename.split('.')[-1]
                                else:
                                    _suffix = ' '
                                if _suffix not in ['fits', 'txt', 'head', 'cl',
                                                   'py', 'pyc', 'pl', 'phot', 'png', 'jpg', 'ps',
                                                   'gz', 'dat', 'lst', 'sh']:
                                    _suffix = 'other'
                                cls._dataproduct = si.DataProduct(filename=filename,
                                                                  relativepath=relativepath,
                                                                  suffix=_suffix,
                                                                  data_type=data_type,
                                                                  subtype=subtype,
                                                                  group=group,
                                                                  filtername=filtername,
                                                                  ra=ra,
                                                                  dec=dec,
                                                                  pointing_angle=pointing_angle)
                                dpowner._dpowner.dataproducts.append(cls._dataproduct)
                                this_nested.commit()
                            else:
                                this_nested.rollback()
                            retry.retry_state.commit()
        else:
            cls._sqlintf_instance_argument()
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'DataProduct')
        # add instance to cache dataframe
        if cls._to_cache:
            cls._to_cache[CLASS_LOW] = cls._inst
            cls.__cache__.loc[len(cls.__cache__)] = cls._to_cache
        new_cls_inst = cls._inst
        delattr(cls, '_inst')
        if old_cls_inst is not None:
            cls._inst = old_cls_inst
        return new_cls_inst

    # @_in_session()
    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_optowner'):
            self._optowner = self._dataproduct
        super(DataProduct, self).__init__(kwargs.get('options', {}))

    @_in_session()
    def __repr__(self):
        cls = self.__class__.__name__
        description = ', '.join([(f"{prop}={getattr(self, prop)}") for prop in [KEYID_ATTR]+UNIQ_ATTRS])
        return f'{cls}({description})'

    @classmethod
    def select(cls, *args, **kwargs):
        """
        Returns a list of DataProduct objects fulfilling the kwargs filter.

        Parameters
        ----------
        kwargs
            Refer to :class:`sqlintf.DataProduct` for parameters.

        Returns
        -------
        out : list of DataProduct object
            list of objects fulfilling the kwargs filter.
        """
        for session in si.begin_session():
            with session as session:
                cls._temp = session.query(si.DataProduct).filter_by(**kwargs)
                for arg in args:
                    cls._temp = cls._temp.filter(arg)
                return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        """
        :obj:`Pipeline`, :obj:`Input` or :obj:`Configuration`: Points to
        attribute self.dpowner.
        """
        return self.dpowner

    @property
    @_in_session()
    def filename(self):
        """
        str: Name of the file the dataproduct points to.
        """
        self._session.refresh(self._dataproduct)
        return _query_return_and_update_cached_row(self, 'filename')

    @filename.setter
    @_in_session()
    def filename(self, filename):
        temp = self.relativepath
        os.rename(temp + '/' + self._dataproduct.filename, temp + '/' + filename)
        self._dataproduct.name = filename
        _temp = _query_return_and_update_cached_row(self, 'filename')
        self.update_timestamp()
        # self._dataproduct.timestamp = datetime.datetime.utcnow()
        # self._session.commit()

    @property
    def filesplitext(self):
        return os.path.splitext(self.filename)

    @property
    @_in_session()
    def dp_id(self):
        """
        int: Primary key id of the table row.
        """
        return self._dataproduct.id

    @property
    @_in_session()
    def relativepath(self):
        """
        str: Path of the directory in which the file the dataproduct points to
        is located.
        """
        return self._dataproduct.relativepath

    @property
    def path(self):
        """
        str: Path where the file the dataproduct points to is located.
        """
        return self.relativepath + '/' + self.filename

    @property
    @_in_session()
    def suffix(self):
        """
        str: Extension of the file the dataproduct points to.
        """
        return self._dataproduct.suffix

    @property
    @_in_session()
    def data_type(self):
        """
        str: Type of the data.
        """
        return self._dataproduct.data_type

    @property
    @_in_session()
    def subtype(self):
        """
        str: Subtype of the data.
        """
        return self._dataproduct.subtype

    @property
    @_in_session()
    def group(self):
        """
        str: Group of the dataproduct ('raw', 'conf', 'log' or 'proc').
        """
        return self._dataproduct.group

    @property
    @_in_session()
    def filtername(self):
        """
        str: Name of the filter of the data.
        """
        return self._dataproduct.filtername

    @property
    @_in_session()
    def ra(self):
        """
        int: Right ascension coordinate of the data.
        """
        return self._dataproduct.ra

    @property
    @_in_session()
    def dec(self):
        """
        int: Declination coordinate of the data.
        """
        return self._dataproduct.dec

    @property
    @_in_session()
    def pointing_angle(self):
        """
        int: Pointing angle coordinate of the data.
        """
        return self._dataproduct.pointing_angle

    @property
    @_in_session()
    def dpowner_id(self):
        """
        int: Primary key id of the table row of parent pipeline, input or
        configuration.
        """
        return self._dataproduct.dpowner_id

    @property
    @_in_session()
    def config_id(self):
        """
        int: Primary key id of the table row of parent configuration - raise
        an AttributeError if the parent is not a Configuration object.
        """
        if self._dataproduct.dpowner.type == 'configuration':
            return self.dpowner_id
        else:
            raise AttributeError

    @property
    @_in_session()
    def input_id(self):
        """
        int: Primary key id of the table row of parent input - raise an
        AttributeError if the parent is not an Input object.
        """
        if self._dataproduct.dpowner.type == 'input':
            return self.dpowner_id
        else:
            raise AttributeError

    @property
    @_in_session()
    def pipeline_id(self):
        """
        int: Primary key id of the table row of parent pipeline.
        """
        if self._dataproduct.dpowner.type == 'pipeline':
            return self.dpowner_id
        else:
            return self.dpowner.pipeline_id

    @property
    @_in_session()
    def dpowner(self):
        """
        :obj:`Input` or :obj:`Configuration`: Input or Configuration object
        corresponding to parent input or configuration.
        """
        if hasattr(self._dataproduct.dpowner, '_wpipe_object'):
            return self._dataproduct.dpowner._wpipe_object
        else:
            if self._dataproduct.dpowner.type == 'configuration':
                from .Configuration import Configuration
                return Configuration(self._dataproduct.dpowner)
            elif self._dataproduct.dpowner.type == 'input':
                from .Input import Input
                return Input(self._dataproduct.dpowner)
            elif self._dataproduct.dpowner.type == 'pipeline':
                from .Pipeline import Pipeline
                return Pipeline(self._dataproduct.dpowner)

    @property
    @_in_session()
    def config(self):
        """
        :obj:`Configuration`: Configuration object corresponding to parent
        configuration - raise an AttributeError if the parent is not a
        Configuration object.
        """
        if self._dataproduct.dpowner.type == 'configuration':
            return self.dpowner
        else:
            raise AttributeError

    @property
    @_in_session()
    def input(self):
        """
        :obj:`Input`: Input object corresponding to parent input - raise an
        AttributeError if the parent is not an Input object.
        """
        if self._dataproduct.dpowner.type == 'input':
            return self.dpowner
        else:
            raise AttributeError

    @property
    @_in_session()
    def pipeline(self):
        """
        :obj:`Pipeline`: Pipeline object corresponding to parent pipeline.
        """
        if self._dataproduct.dpowner.type == 'pipeline':
            return self.dpowner
        else:
            return self.dpowner.pipeline

    @property
    def target(self):
        """
        :obj:`Target`: Target object corresponding to parent target - raise an
        AttributeError if the owner is not a Configuration object.
        """
        return self.config.target

    @property
    def target_id(self):
        """
        int: Primary key id of the table row of parent target - raise an
        AttributeError if the owner is not a Configuration object.
        """
        return self.config.target_id

    def _prep_copy_symlink(self, path, kwargs):  # TODO case if path is base+filename and if path is DP
        path = clean_path(path)
        if os.path.exists(path):
            filename = self.filename
        else:
            path, filename = os.path.split(path)
        newkwargs = return_dict_of_attrs(self._dataproduct)
        for key in list(newkwargs.keys()):
            if key[-2:] == 'id' or key in ['type', 'timestamp']:
                del newkwargs[key]
        for key in kwargs.keys():
            newkwargs[key] = kwargs[key]
        newkwargs['filename'] = filename
        newkwargs['relativepath'] = path
        return newkwargs
    def _copy_symlink(self, path, kwargs, func):
        dpowner = kwargs.pop('dpowner', self.dpowner)
        return_dp = kwargs.pop('return_dp', True)
        kwargs = self._prep_copy_symlink(path, kwargs)
        filename = kwargs['filename']
        path = kwargs['relativepath']
        if os.path.exists(path + '/' + filename):
            selection = DataProduct.select(relativepath=path, filename=filename)
            if len(selection):  # TAKE OLDEST ONE? CHECK TIMESTAMPS?
                new_dp = selection[0]
        if 'new_dp' not in locals():
            new_dp = None
            if '.'.join(type(dpowner).__module__.split('.')[:-1]) == 'wpipe.sqlintf':
                dpowner.dataproducts.append(si.DataProduct(**kwargs))
                if return_dp:
                    new_dp = DataProduct(dpowner.dataproducts[-1])
            elif '.'.join(type(dpowner).__module__.split('.')[:-1]) == 'wpipe':
                if return_dp:
                    new_dp = dpowner.dataproduct(**kwargs)
            else:
                raise TypeError
        if not os.path.exists(path + '/' + filename):
            func(self.path, path + '/' + filename)
        return new_dp

    def make_copy(self, path, **kwargs):
        """
        Create a copy at the given path of this dataproduct file.

        Parameters
        ----------
        path : str
            Path where to create the copy.
        kwargs
            Refer to :class:`DataProduct` for other parameters.

        Returns
        -------
        new_dp : :obj:`DataProduct`
            New associated dataproduct.

        Notes
        -----
        A dpowner argument must be given to the kwargs to associate the new
        dataproduct to a different dpowner. Otherwise, it will be associated
        to this dataproduct owner.
        """
        return self._copy_symlink(path, kwargs, func=shutil.copy2)

    def symlink(self, path, **kwargs):
        """
        Create a symbolic link at the given path to this dataproduct file.

        Parameters
        ----------
        path : str
            Path where to create the symbolic link.
        kwargs
            Refer to :class:`DataProduct` for other parameters.

        Returns
        -------
        new_dp : :obj:`DataProduct`
            New associated dataproduct.

        Notes
        -----
        A dpowner argument must be given to the kwargs to associate the new
        dataproduct to a different dpowner. Otherwise, it will be associated
        to this dataproduct owner.
        """
        return self._copy_symlink(path, kwargs, func=os.symlink)

    def open(self, *args, **kwargs):
        """
        Open dataproduct file and returns a stream.

        Parameters
        ----------
        args, kwargs
            Refer to built-in function :func:`open`

        Returns
        -------
        file
            File object
        """
        return open(self.path, *args, **kwargs)

    def remove_data(self):
        """
        Remove dataproduct's file.
        """
        remove_path(self.path)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        try:
            self.remove_data()
        except TypeError:
            pass
        super(DataProduct, self).delete()
        self.__class__.__cache__ = self.__cache__[self.__cache__[CLASS_LOW] != self]
