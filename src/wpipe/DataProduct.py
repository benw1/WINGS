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
        # checking if given argument is sqlintf object
        cls._dataproduct = args[0] if len(args) else None
        if not isinstance(cls._dataproduct, si.DataProduct):
            # gathering construction arguments
            config = kwargs.get('config', args[0] if len(args) else None)
            filename = kwargs.get('filename', args[1] if len(args) > 1 else None)
            relativepath = kwargs.get('relativepath', config.relativepath)
            group = kwargs.get('group', '')
            data_type = kwargs.get('data_type', '')
            subtype = kwargs.get('subtype', '')
            filtername = kwargs.get('filtername', '')
            ra = kwargs.get('ra', 0)
            dec = kwargs.get('dec', 0)
            pointing_angle = kwargs.get('pointing_angle', 0)
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
    def target_id(self):
        si.session.commit()
        return self._dataproduct.config.target_id

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._dataproduct.config.target.pipeline_id
