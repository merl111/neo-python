import os
import shutil

from neo.Storage.Implementation.DBFactory import getBlockchainDB
from neo.Storage.Interface.DBInterface import DBProperties
from neo.Settings import settings
from neo.logging import log_manager
from pathlib import Path


logger = log_manager.getLogger()

def migrateDB(fromdb, todb, path, remove_old=False):

    if os.path.exists(path+'/MIGRATED'):
        logger.info('Migration already done')
        return

    # move directory to temp dir
    temp_path = path+'_temp_migrate'
    shutil.move(path, temp_path)

    # create old path again
    os.mkdir(path)

    # open DBs
    mig_db_from = getBlockchainDB(temp_path, fromdb)
    mig_db_to = getBlockchainDB(path, todb)

    # now read EVERYTHING from "fromdb" and write to "todb"
    with mig_db_from.openIter(DBProperties()) as iterator:
        for key, value in iterator:
            mig_db_to.write(key, value)

    mig_db_from.closeDB()

    if remove_old:
        shutil.rmtree(temp_path)

    mig_db_to.closeDB()
    open(path+'/MIGRATED', 'a').close()
