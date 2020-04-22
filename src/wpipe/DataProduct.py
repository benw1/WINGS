from .core import *
from .OptOwner import OptOwner


class DataProduct(OptOwner):
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
                group = kwargs.get('group', '' if args[2] is None else args[2])
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
        return self.config

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
