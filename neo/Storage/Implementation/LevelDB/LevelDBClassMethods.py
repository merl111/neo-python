import plyvel
import threading
import struct
import time

from contextlib import contextmanager

from neo.Core.Blockchain import Blockchain
from neo.Storage.Common.DBPrefix import DBPrefix
from neo.Storage.Interface.DBInterface import DBProperties
from neo.logging import log_manager


logger = log_manager.getLogger('LevelDB')

"""Document me"""

_init_method = '_db_init'

_path = None

_db = None

_iter = None

_snapshot = None

_batch = None

_lock = threading.Lock()


@property
def Path(self):
    return self._path


def _db_init(self, path):
    try:
        self._path = path
        self._db = plyvel.DB(path, create_if_missing=True)
        logger.info("Created Blockchain DB at %s " % self._path)
    except Exception as e:
        raise Exception("leveldb exception [ %s ]" % e)


def write(self, key, value):
    self._db.put(key, value)


def writeBatch(self, batch: dict):
    with self._db.write_batch() as wb:
        for key, value in batch.items():
            wb.put(key, value)


def get(self, key, default=None):
    _value = self._db.get(key, default)
    return _value[:-4]


def delete(self, key):
    self._db.delete(key)


def deleteBatch(self, batch: dict):
    with self._db.write_batch() as wb:
        for key in batch:
            wb.delete(key)


def cloneDatabase(self, clone_db):
    db_snapshot = self.createSnapshot()
    for key, value in db_snapshot.iterator(prefix=DBPrefix.ST_Storage, include_value=True):
        clone_db.write(key, value)
    return clone_db


def createSnapshot(self):
    self._snapshot = self._db.snapshot()
    return self._snapshot


@contextmanager
def openIter(self, properties):
    # TODO start implement start and end

    self._iter = self._db.iterator(
                                    prefix=properties.prefix,
                                    include_value=properties.include_value,
                                    include_key=properties.include_key)
    yield self._iter
    self._iter.close()


@contextmanager
def getBatch(self):
    with _lock:
        self._batch = self._db.write_batch()
        yield self._batch
        self._batch.write()


def rollbackDatabase(self, to_block_id):

    import binascii
    from neo.Core.Block import Block
    from neo.Core.Header import Header

    current_block = None
    current_header = None
    for key, value in self._db.iterator(prefix=DBPrefix.DATA_Block,
                                        include_value=True, reverse=True):
        try:

            outhex = binascii.unhexlify(bytearray(value)[8:])
            index = struct.unpack('<I', outhex[72:76])[0]

            logger.info('check index %d', index)
            if index == to_block_id:
                block = Block.FromTrimmedData(outhex)
                current_block = block
                current_header = Header.FromTrimmedData(outhex, 0)
                logger.info('found current block %d', block.Index)
            elif index > to_block_id:
                block = Block.FromTrimmedData(outhex)
                logger.info('removing block %d', block.Index)
                self.delete(key)
        except Exception as e:
            pass
            # logger.info("Could not get block %s " % e)

    self.write(DBPrefix.SYS_CurrentBlock, current_block.Hash.ToBytes() + current_block.IndexBytes())
    self.write(DBPrefix.SYS_CurrentHeader, current_header.Hash.ToBytes() + current_header.Index.to_bytes(4, 'little'))
    self.delete(DBPrefix.IX_HeaderHashList)


def closeDB(self):
    self._db.close()
