from .core import *
from .Store import Store

class Options():
    def __init__(self, opts={'any': 0}):
        self.__dict__ = opts
        return None

    def new(self, owner=str('any' + 250 * ' '), owner_id=0):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name, value]).T, columns=
        ['name', 'value']).sort_values('name')
        owner = np.repeat(str(owner), len(name))
        owner_id = np.repeat(int(owner_id), len(name))
        arrays = [owner, owner_id]
        _df.index = pd.MultiIndex.from_arrays(arrays, names=('owner', 'owner_id'))
        return _df

    def create(self, owner='any', owner_id=0, store=Store()):
        _df = self.new(owner, owner_id)
        with pd.HDFStore(str(store.path), 'r+') as myStore:
            myStore.append('options', _df, min_itemsize=fmin_itemsize(_df),
                           complevel=9, complib='blosc:blosclz')
        return _df

    def get(owner, owner_id, store=Store()):
        x = store.select('options').loc[str(owner)].loc[int(owner_id)]
        if x.shape == (2,):
            return dict(zip([x['name']], [x['value']]))
        else:
            return dict(zip(x['name'].values, x['value'].values))

    def addOption(owner, owner_id, key, value, store=Store()):
        _opt = Options.get(owner, int(owner_id))
        _opt[key] = value
        return store.update('options', Options(_opt).new(owner, int(owner_id)))