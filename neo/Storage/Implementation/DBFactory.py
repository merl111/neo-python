from neo.Storage.Interface.AbstractDBInterface import AbstractDBInterface
from neo.Settings import settings
from neo.logging import log_manager


"""Module is used to access the different databases.
Import the module and use the getters to  access the different databases.
Configuration is done in neo.Settings.DATABASE_PROPS dict.
"""

# logger = log_manager.getLogger('DBFactory')
logger = log_manager.getLogger()

BC_CONST = 'blockchain'
NOTIF_CONST = 'notification'
DEBUG_CONST = 'debug'

DATABASE_PROPS = settings.database_properties()

_blockchain_db_instance = None

_notif_db_instance = None

_debug_db_instance = None


def getBlockchainDB(path=None):

    if not path:
        path = DATABASE_PROPS[BC_CONST]['path']

    BlockchainDB = _dbFactory(BC_CONST, DATABASE_PROPS[BC_CONST])
    _blockchain_db_instance = BlockchainDB(path)
    return _blockchain_db_instance


def getNotificationDB(path=None):

    if not path:
        path = DATABASE_PROPS[NOTIF_CONST]['path']

    if DATABASE_PROPS[NOTIF_CONST]['backend'] == 'rocksdb':
        raise Exception('Not yet possible, please use leveldb!')

    NotificationDB = _dbFactory(NOTIF_CONST, DATABASE_PROPS[NOTIF_CONST])
    _notif_db_instance = NotificationDB(path)
    return _notif_db_instance


def getDebugStorageDB():
    DebugStorageDB = _dbFactory(DEBUG_CONST, DATABASE_PROPS[DEBUG_CONST])
    _debug_db_instance = DebugStorageDB(DATABASE_PROPS[DEBUG_CONST]['path'])
    return _debug_db_instance


def _dbFactory(dbType, properties):

        if properties['backend'] == 'leveldb':
            import neo.Storage.Implementation.LevelDB.LevelDBClassMethods as functions
        elif properties['backend'] == 'rocksdb':
            import neo.Storage.Implementation.RocksDB.RocksDBClassMethods as functions

        methods = [x for x in dir(functions) if not x.startswith('__')]

        # build attributes dict
        attributes = {methods[i]: getattr(
            functions, methods[i]) for i in range(0, len(methods))}

        # add __init__ method
        attributes['__init__'] = attributes.pop(functions._init_method)

        # print(attributes)

        return type(
            properties['backend'].title() + 'DBImpl' + dbType.title(),
            (AbstractDBInterface,),
            attributes)
