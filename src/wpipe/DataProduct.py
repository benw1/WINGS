#!/usr/bin/env python
"""
Contains the DataProduct class definition

Please note that this module is private. The DataProduct class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import *
from .OptOwner import OptOwner


class DataProduct(OptOwner):
    """
        A DataProduct object represents a data file in the pipeline, either
        owned by a pipeline input or a target configuration, defined by its
        parent input or configuration, its filename and its group.

        Construction
        ------------

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
        (input or configuration), their filename, and their group, but
        alternatively, the constructor can take as sole argument either:
         - the primary key id of the corresponding `dataproducts` table row
         - the `sqlintf.DataProduct` object interfacing that table row

        After the instantiation of __new__ is completed, if a dictionary of
        options was given to the constructor, the __init__ method constructs
        a set of Option objects owned by the dataproduct.

        Parameters
        ----------
        dpowner : Input or Configuration object
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
        parents : Input or Configuration object
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
            Primary key id of the table row of parent input or configuration.
        config_id : int
            Primary key id of the table row of parent configuration - raise an
            AttributeError if the parent is an Input object.
        input_id : int
            Primary key id of the table row of parent input - raise an
            AttributeError if the parent is a Configuration object.
        dpowner : Input or Configuration object
            Input or Configuration object corresponding to parent input or
            configuration.
        config : Configuration object
            Configuration object corresponding to parent configuration - raise
            an AttributeError if the parent is an Input object.
        input : Input object
            Input object corresponding to parent input - raise an
            AttributeError if the parent is a Configuration object.
        target : Target object
            Target object corresponding to parent target - raise
            an AttributeError if the owner is an Input object.
        target_id : int
            Primary key id of the table row of parent target - raise an
            AttributeError if the owner is an Input object.
        pipeline_id : int
            Primary key id of the table row of parent pipeline.
        optowner_id : int
            Points to attribute dp_id.
        options : core.DictLikeChildrenProxy object
            Dictionary of Option objects owned by the target.

        How to use
        ----------
        A DataProduct object constructs from dataproduct owner, that can be
        either an Input object or a Configuration object: this can be
        achieved either by using the dataproduct generating object method of
        such dataproduct owner object, or alternatively by using the
        DataProduct class constructor giving it the dataproduct owner object
        (Input or Configuration) as argument. In both cases, the signature
        must also contain the filename of the data file as well as its group:

        >>> my_dp = my_input.dataproduct(filename, group)
        or
        >>> my_dp = my_config.dataproduct(filename, group)
        or
        >>> my_dp = wp.DataProduct(my_input, filename, group)
        or
        >>> my_dp = wp.DataProduct(my_config, filename, group)
    """
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._dataproduct = args[0] if len(args) else None
        if not isinstance(cls._dataproduct, si.DataProduct):
            keyid = kwargs.get('id', cls._dataproduct)
            if isinstance(keyid, int):
                cls._dataproduct = si.session.query(si.DataProduct).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=9)
                list(wpargs.__setitem__('DPOwner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.DPOwner.__subclasses__())))
                dpowner = kwargs.get('dpowner', wpargs.get('DPOwner', None))
                filename = kwargs.get('filename', args[0])
                relativepath = clean_path(kwargs.get('relativepath', args[1]))
                group = kwargs.get('group', args[2])
                data_type = kwargs.get('data_type', '' if args[3] is None else args[3])
                subtype = kwargs.get('subtype', '' if args[4] is None else args[4])
                filtername = kwargs.get('filtername', '' if args[5] is None else args[5])
                ra = kwargs.get('ra', 0 if args[6] is None else args[6])
                dec = kwargs.get('dec', 0 if args[7] is None else args[7])
                pointing_angle = kwargs.get('pointing_angle', 0 if args[8] is None else args[8])
                # querying the database for existing row or create
                try:
                    cls._dataproduct = si.session.query(si.DataProduct). \
                        filter_by(dpowner_id=dpowner.dpowner_id). \
                        filter_by(group=group). \
                        filter_by(filename=filename).one()
                except si.orm.exc.NoResultFound:
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
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'DataProduct')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_optowner'):
            self._optowner = self._dataproduct
        super(DataProduct, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.DataProduct).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.dpowner

    @property
    def filename(self):
        si.session.commit()
        return self._dataproduct.filename

    @filename.setter
    def filename(self, filename):
        os.rename(self.relativepath+'/'+self._dataproduct.filename, self.relativepath+'/'+filename)
        self._dataproduct.name = filename
        self._dataproduct.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def dp_id(self):
        si.session.commit()
        return self._dataproduct.id

    @property
    def relativepath(self):
        si.session.commit()
        return self._dataproduct.relativepath

    @property
    def path(self):
        return self.relativepath+'/'+self.filename

    @property
    def suffix(self):
        si.session.commit()
        return self._dataproduct.suffix

    @property
    def data_type(self):
        si.session.commit()
        return self._dataproduct.data_type

    @property
    def subtype(self):
        si.session.commit()
        return self._dataproduct.subtype

    @property
    def group(self):
        si.session.commit()
        return self._dataproduct.group

    @property
    def filtername(self):
        si.session.commit()
        return self._dataproduct.filtername

    @property
    def ra(self):
        si.session.commit()
        return self._dataproduct.ra

    @property
    def dec(self):
        si.session.commit()
        return self._dataproduct.dec

    @property
    def pointing_angle(self):
        si.session.commit()
        return self._dataproduct.pointing_angle

    @property
    def dpowner_id(self):
        si.session.commit()
        return self._dataproduct.dpowner_id

    @property
    def config_id(self):
        if self._dataproduct.dpowner.type == 'configuration':
            return self.dpowner_id
        else:
            raise AttributeError

    @property
    def input_id(self):
        if self._dataproduct.dpowner.type == 'input':
            return self.dpowner_id
        else:
            raise AttributeError

    @property
    def dpowner(self):
        if hasattr(self._dataproduct.dpowner, '_wpipe_object'):
            return self._dataproduct.dpowner._wpipe_object
        else:
            if self._dataproduct.dpowner.type == 'configuration':
                from .Configuration import Configuration
                return Configuration(self._dataproduct.dpowner)
            elif self._dataproduct.dpowner.type == 'input':
                from .Input import Input
                return Input(self._dataproduct.dpowner)

    @property
    def config(self):
        if self._dataproduct.dpowner.type == 'configuration':
            return self.dpowner
        else:
            raise AttributeError

    @property
    def input(self):
        if self._dataproduct.dpowner.type == 'input':
            return self.dpowner
        else:
            raise AttributeError

    @property
    def target(self):
        return self.config.target

    @property
    def target_id(self):
        return self.config.target_id

    @property
    def pipeline_id(self):
        return self.dpowner.pipeline_id

    def symlink(self, path, **kwargs):
        dpowner = kwargs.pop('dpowner', self.dpowner)
        path = clean_path(path)
        if os.path.exists(path):
            filename = self.filename
        else:
            path, filename = os.path.split(path)
        if not os.path.exists(path+'/'+filename):
            kwargs['filename'] = filename
            kwargs['relativepath'] = path
            if '.'.join(type(dpowner).__module__.split('.')[:-1]) == 'wpipe.sqlintf':
                dpowner.dataproducts.append(si.DataProduct(**kwargs))
            elif '.'.join(type(dpowner).__module__.split('.')[:-1]) == 'wpipe':
                dpowner.dataproduct(**kwargs)
            else:
                raise TypeError
            os.symlink(self.path, path+'/'+filename)
