from .core import *

class Store():
    def __init__(self, storePath=path_to_store):
        self.path = str(storePath)
        return None

    def new(self):
        from . import User, Node, Options, Pipeline, Target,\
            Configuration, DataProduct, Parameters, Task, Job, Mask, Event
        _dict = {'users': User().new(),
                 'nodes': Node().new(),
                 'options': Options().new(),
                 'pipelines': Pipeline().new(),
                 'targets': Target().new(),
                 'configurations': Configuration().new(),
                 'data_products': DataProduct().new(),
                 'parameters': Parameters().new(),
                 'tasks': Task().new(),
                 'jobs': Job().new(),
                 'masks': Mask().new(),
                 'events': Event().new()}
        with pd.HDFStore(str(self.path), 'w', complevel=9,
                         complib='blosc:blosclz') as myStore:
            for k, v in _dict.items():
                myStore.append(k, v, min_itemsize=fmin_itemsize(v),
                               complevel=9, complib='blosc:blosclz')
        return None

    def create(self, key, name_id, stuff):
        with pd.HDFStore(str(self.path), 'r+') as myStore:
            stuff.__dict__[name_id][0] = int(myStore[key][name_id].max()) + 1
            newStuff = stuff.new()
            myStore.append(key, newStuff, min_itemsize=fmin_itemsize(newStuff),
                           complevel=9, complib='blosc:blosclz')
        return newStuff

    def update(self, key, stuff):
        with pd.HDFStore(str(self.path), 'r+') as myStore:
            _t = myStore[key]
            _t = _t.drop(index=stuff.index).append(stuff)
            myStore.remove(key)
            myStore.append(key, _t, min_itemsize=fmin_itemsize(_t),
                           complevel=9, complib='blosc:blosclz')
        return None

    def select(self, key='events', where='all', columns=None):
        with pd.HDFStore(str(self.path), 'r') as myStore:
            if where == 'all':
                if columns is None:
                    return myStore.get(str(key))
                else:
                    return myStore.select(str(key), columns=columns)
            else:
                return myStore.select(str(key), columns=columns).query(str(where))

    def repack(self):
        filename = str(self.path)
        _t1 = ['cp', filename, './backup1.h5']
        _t2 = ['ptrepack', '--chunkshape=auto', '--propindexes', '--complevel=9',
               '--complib=blosc', filename, './temp1.h5']
        _t3 = ['mv', './temp1.h5', filename]
        _t = subprocess.run(_t1, stdout=subprocess.PIPE)
        _t = subprocess.run(_t2, stdout=subprocess.PIPE)
        _t = subprocess.run(_t3, stdout=subprocess.PIPE)
        return