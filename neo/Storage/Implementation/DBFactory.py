from neo.Storage.Implementation.AbstractDBImplementation import (
    AbstractDBImplementation
)
from neo.Settings import settings
from neo.logging import log_manager


""" 
Database factory module

Note: Module is used to access the different database implementations.
Import the module and use the getters to access the different databases.
Configuration is done in neo.Settings.DATABASE_PROPS dict.
Each getter returns an instance of the database.

"""

logger = log_manager.getLogger()

BC_CONST = 'blockchain'
NOTIF_CONST = 'notification'
DEBUG_CONST = 'debug'

DATABASE_PROPS = settings.database_properties()

_blockchain_db_instance = None

_notif_db_instance = None

_debug_db_instance = None


def getBlockchainDB(path=None, dbType=None):
    """
    Returns a database instance used with the blockchain class.
    """

    if not path:
        path = DATABASE_PROPS[BC_CONST]['path']

    if not dbType:
        dbType = DATABASE_PROPS[BC_CONST]['backend']

    BlockchainDB = _dbFactory(BC_CONST, dbType)
    _blockchain_db_instance = BlockchainDB(path)
    return _blockchain_db_instance


def getNotificationDB(path=None):
    """
    Returns a database instance used with the notification class.
    """

    if not path:
        path = DATABASE_PROPS[NOTIF_CONST]['path']

    if DATABASE_PROPS[NOTIF_CONST]['backend'] == 'rocksdb':
        raise Exception('Not yet possible, please use leveldb!')

    NotificationDB = _dbFactory(NOTIF_CONST, DATABASE_PROPS[NOTIF_CONST]['backend'])
    _notif_db_instance = NotificationDB(path)
    return _notif_db_instance


def getDebugStorageDB():
    """
    Returns a database instance used with the debug storage class.
    """
    DebugStorageDB = _dbFactory(DEBUG_CONST, DATABASE_PROPS[DEBUG_CONST]['backend'])
    _debug_db_instance = DebugStorageDB(DATABASE_PROPS[DEBUG_CONST]['path'])
    return _debug_db_instance


def _dbFactory(dbType, backend):

    functions = None
    if backend == 'leveldb':
        """
        Module implements the methods used by the dynamically generated class.
        """
        import neo.Storage.Implementation.LevelDB.LevelDBClassMethods as functions
    elif backend == 'rocksdb':
        import neo.Storage.Implementation.RocksDB.RocksDBClassMethods as functions
    else:
        raise Exception('Unsupported backend [%s] configured!', backend)

    methods = [x for x in dir(functions) if not x.startswith('__')]

    # build the dict containing all the attributes (methods + members)
    attributes = {methods[i]: getattr(
        functions, methods[i]) for i in range(0, len(methods))}

    # add __init__ method
    attributes['__init__'] = attributes.pop(functions._init_method)

    return type(
        backend.title() + 'DBImpl' + dbType.title(),
        (AbstractDBImplementation,),
        attributes)
