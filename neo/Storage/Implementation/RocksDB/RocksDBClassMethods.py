import rocksdb
import threading
import itertools

from contextlib import contextmanager

from neo.Core.Blockchain import Blockchain
from neo.Storage.Common.DBPrefix import DBPrefix
from neo.Storage.Interface.DBInterface import DBProperties
from neo.logging import log_manager

from .StaticPrefix import StaticPrefix


logger = log_manager.getLogger()

"""Document me"""

_init_method = '_db_init'
_prefix_init_method = '_prefix_db_init'
_snap_init_method = '_snap_db_init'

_path = None

_db = None
_snapshot = None

_lock = threading.RLock()


@property
def Path(self):
    return self._path


def _prefix_db_init(self, _prefixdb):
    try:
        self._db = _prefixdb
    except Exception as e:
        raise Exception("rocksdb exception [ %s ]" % e)

def _snap_db_init(self, db):
    try:
        self._db = db
        self._snapshot = db.snapshot()
    except Exception as e:
        raise Exception("rocksdb exception [ %s ]" % e)


def _db_init(self, path):
    try:
        self._path = path
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.prefix_extractor = StaticPrefix()
        self._db = rocksdb.DB(path, opts)
        logger.info("Created DB at %s " % self._path)
    except Exception as e:
        raise Exception("rocksdb exception [ %s ]" % e)


def write(self, key, value):
    self._db.put(key, value)


def writeBatch(self, batch: dict):
    with self._db.write_batch() as wb:
        for key, value in batch.items():
            wb.put(key, value)


def get(self, key, default=None):
    _res = self._db.get(key)
    if _res:
        return _res
    else:
        return default


def delete(self, key):
    self._db.delete(key)


def deleteBatch(self, batch: dict):
    with self._db.write_batch() as wb:
        for key in batch:
            wb.delete(key)


def cloneDatabase(self, clone_db):
    db_snapshot = self.createSnapshot()
    with db_snapshot.openIter(DBProperties(prefix=DBPrefix.ST_Storage, include_value=True)) as iterator:
        for key, value in iterator:
            clone_db.write(key, value)
    return clone_db


def createSnapshot(self):
    # check if snapshot db has to be closed
    from .InternalDBFactory import internalDBFactory
    SnapshotDB = internalDBFactory('Snapshot')
    return SnapshotDB(self._db)


@contextmanager
def openIter(self, properties):

    key = properties.include_key
    value = properties.include_value
    prefix = properties.prefix

    _iter = self._db.iteritems()
    _res = None

    if prefix:
        _iter.seek(prefix)
        if value and key:
            _res = dict(
                    itertools.takewhile(
                        lambda item: item[0].startswith(prefix), _iter)
                    ).items()
        elif value:
            _res = list(
                    dict(
                        itertools.takewhile(
                            lambda item: item[0].startswith(prefix), _iter)
                        ).values()
                    )
        elif key:
            _res = list(
                    dict(
                        itertools.takewhile(
                            lambda item: item[0].startswith(prefix), _iter)
                        ).keys()
                    )

    else:
        _iter.seek_to_first()
        if value and key:
            _res = dict(_iter).items()
        elif value:
            _res = list(dict(_iter).values())
        elif key:
            _res = list(dict(_iter).keys())

    yield _res

    del _iter
    del _res


@contextmanager
def getBatch(self):
    with _lock:
        _batch = rocksdb.WriteBatch()
        yield _batch
        self._db.write(_batch)


def getPrefixedDB(self, prefix):

    # check if prefix db has to be closed
    # from .InternalDBFactory import internalDBFactory

    # PrefixedDB = internalDBFactory('Prefixed')
    # return PrefixedDB(self._db.prefixed_db(prefix))
    raise NotImplementedError


def closeDB(self):
    del self._db
