from .core import *
from .Owner import Owner


class DataProduct(Owner):
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
                config = kwargs.get('config', wpargs.get('Configuration', None))
                filename = kwargs.get('filename', args[0])
                relativepath = clean_path(kwargs.get('relativepath', config.datapath if args[1] is None else args[1]))
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
                        filter_by(config_id=config.config_id). \
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
                    config._configuration.dataproducts.append(cls._dataproduct)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'DataProduct')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_owner'):
            self._owner = self._dataproduct
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
    def config_id(self):
        si.session.commit()
        return self._dataproduct.config_id

    @property
    def config(self):
        if hasattr(self._dataproduct.config, '_wpipe_object'):
            return self._dataproduct.config._wpipe_object
        else:
            from .Configuration import Configuration
            return Configuration(self._dataproduct.config)

    @property
    def target(self):
        return self.config.target

    @property
    def target_id(self):
        return self.config.target_id

    @property
    def pipeline_id(self):
        return self.config.pipeline_id
