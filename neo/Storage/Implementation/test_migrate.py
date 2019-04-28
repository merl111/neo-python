import os 
import shutil
import tarfile
import requests

from unittest import TestCase, skip
from neo.Storage.Implementation.DBMigrate import migrateDB
from neo.Settings import settings
from neo.logging import log_manager

logger = log_manager.getLogger()


class LevelDBTest(TestCase):

    FIXTURE_REMOTE_LOC = 'https://s3.us-east-2.amazonaws.com/cityofzion/fixtures/fixtures_v8.tar.gz'
    FIXTURE_FILENAME = os.path.join(settings.DATA_DIR_PATH, 'Chains/fixtures_v8.tar.gz')
    TEST_CHAIN = os.path.join(settings.DATA_DIR_PATH, 'fixtures/test_chain')

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.TEST_CHAIN)

    def test_migration(self):

        # setup Blockchain DB
        if not os.path.exists(self.FIXTURE_FILENAME):
            logger.info(
                "downloading fixture block database from %s. this may take a while" % self.FIXTURE_REMOTE_LOC)

            response = requests.get(self.FIXTURE_REMOTE_LOC, stream=True)

            response.raise_for_status()
            os.makedirs(os.path.dirname(self.FIXTURE_FILENAME), exist_ok=True)
            with open(self.FIXTURE_FILENAME, 'wb+') as handle:
                for block in response.iter_content(1024):
                    handle.write(block)

        try:
            tar = tarfile.open(self.FIXTURE_FILENAME)
            tar.extractall(path=settings.DATA_DIR_PATH)
            tar.close()
        except Exception as e:
            raise Exception(
                "Could not extract tar file - %s. You may want need to remove the fixtures file %s manually to fix this." % (e, self.FIXTURE_FILENAME))

        if settings.get_db_backend != 'leveldb':
            migrateDB(fromdb='leveldb', todb=settings.get_db_backend(), path=self.TEST_CHAIN, remove_old=True)
