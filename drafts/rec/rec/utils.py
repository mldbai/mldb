import os, logging
import pymldb
from rec.settings import DATA_DIR

log = logging.getLogger(__name__)

def dataset_conf(name, type_=None, **params):
    conf = {'id': name, 'type': type_}
    conf['params'] = params
    if type_ == 'beh.mutable':
        conf['params'] = {
            'dataFileUrl': os.path.join(DATA_DIR, name + '.beh')}
    elif type_ is None:
        conf.pop('type')
    # else:
    #     raise ValueError('unknow type ' + type_)
    return conf


def load_from_beh_or_import(mldb, conf, import_fn, *args, **kwargs):
    """ if dataset_id is a beh.mutable, we will try to load the .beh file
        if it's already present
        if it's not we will call `import_fn` with the remaining args
    """
    if not conf['type'] == 'beh.mutable':
        raise ValueError('type must be beh.mutable... yes?')
    fileurl = conf['params']['dataFileUrl']
    print 'fileurl', fileurl
    try:
        log.debug('trying to load .beh file')
        mldb.create_dataset(conf['id'], 'beh', dataFileUrl=fileurl)
    except pymldb.resource.ResourceError:
        log.info('importing data to %s', conf['id'])
        import_fn(*args, **kwargs)
