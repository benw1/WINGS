from .core import *
from .Store import Store
from .Owner import SQLOwner
from .Configuration import Configuration


class DataProduct:
    def __init__(self, filename='any', relativepath='', group='',
                 configuration=Configuration().new(),
                 data_type='', subtype='', filtername='',
                 ra=0, dec=0, pointing_angle=0):
        self.config_id = np.array([int(configuration.config_id)])
        self.target_id = np.array([int(configuration.target_id)])
        self.pipeline_id = np.array([int(configuration.pipeline_id)])
        self.dp_id = np.array([int(0)])

        self.filename = np.array([str(filename)])
        self.relativepath = np.array([str(relativepath)])

        _suffix = ' '
        if '.' in filename:
            _suffix = filename.split('.')[-1]
        if _suffix not in ['fits', 'txt', 'head', 'cl',
                           'py', 'pyc', 'pl', 'phot', 'png', 'jpg', 'ps',
                           'gz', 'dat', 'lst', 'sh']:
            _suffix = 'other'
        self.suffix = np.array([str(_suffix)])

        if not data_type:
            data_type = _suffix
        self.data_type = np.array([str(data_type)])
        self.subtype = np.array([str(subtype)])

        if group not in ['proc', 'conf', 'log', 'raw']:
            group = 'other'
        self.group = np.array([str('other')])

        self.filtername = np.array([str(filtername)])
        self.ra = np.array([float(ra)])
        self.dec = np.array([float(dec)])
        self.pointing_angle = np.array([float(pointing_angle)])
        # self.tags = Options(tags) # meant to break
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)]), np.array([int(self.config_id)]),
                                                      np.array([int(self.dp_id)])],
                                              names=('pipelineID', 'targetID', 'configID', 'dpID'))
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, store=Store()):
        from . import Options
        _df = store.create('data_products', 'dp_id', self)
        _opt = Options(options).create('data_product', int(_df.dp_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(dp_id, store=Store()):
        x = store.select('data_products', 'dp_id==' + str(dp_id))
        return x.loc[x.index.values[0]]


class SQLDataProduct(SQLOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._dataproduct = args[0] if len(args) else None
        if not isinstance(cls._dataproduct, si.DataProduct):
            id = kwargs.get('id', cls._dataproduct)
            if isinstance(id, int):
                cls._dataproduct = si.session.query(si.DataProduct).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args = wpargs_from_args(*args)
                config = wpargs.get('Configuration', kwargs.get('config', None))
                filename = args[0] if len(args) else kwargs.get('filename', None)
                relativepath = args[1] if len(args) > 1 else kwargs.get('relativepath', config.relativepath)
                group = args[2] if len(args) > 2 else kwargs.get('group', '')
                data_type = args[3] if len(args) > 3 else kwargs.get('data_type', '')
                subtype = args[4] if len(args) > 4 else kwargs.get('subtype', '')
                filtername = args[5] if len(args) > 5 else kwargs.get('filtername', '')
                ra = args[6] if len(args) > 6 else kwargs.get('ra', 0)
                dec = args[7] if len(args) > 7 else kwargs.get('dec', 0)
                pointing_angle = args[8] if len(args) > 8 else kwargs.get('pointing_angle', 0)
                # querying the database for existing row or create
                try:
                    cls._dataproduct = si.session.query(si.DataProduct). \
                        filter_by(config_id=config.config_id). \
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
                    config._config.dataproducts = cls._dataproduct
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'DataProduct', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_owner'):
            super().__init__()
            self._owner = self._dataproduct
        self.options = kwargs.get('options', {})
        self._dataproduct.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parents(self):
        return self.config

    @property
    def filename(self):
        si.session.commit()
        return self._dataproduct.filename

    @filename.setter
    def filename(self, filename):
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
            from .Configuration import SQLConfiguration
            return SQLConfiguration(self._dataproduct.config)

    @property
    def target_id(self):
        return self.config.target_id

    @property
    def pipeline_id(self):
        return self.config.pipeline_id
