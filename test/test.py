'''
Created on Oct 25, 2012

@author: jluker
'''

import os
import tempfile
from stat import *
from time import sleep
from unittest import TestCase, main
from datetime import datetime

from config import config
from mongodb import mongodb,models,utils

class AdsdataTestCase(TestCase):
    
    def setUp(self):
        config.MONGO_DATABASE = 'test'
        config.MONGO_HOST = 'localhost'
        mongo = mongodb.get_mongo()
        mongo.db.connection.drop_database('test')
    
    def tearDown(self):
        mongo = mongodb.get_mongo()
    
class BasicCollection(models.DataCollection):
    config_collection_name = 'adsdata_test'
    foo = mongodb.StringField(_id=True)
    bar = mongodb.StringField()
    field_order = [foo, bar]
            
class AggregatedCollection(models.DataCollection):
    config_collection_name = 'adsdata_test'
    foo = mongodb.StringField(_id=True)
    bar = mongodb.ListField(mongodb.StringField())
    aggregated = True
    field_order = [foo, bar]
    
class TestDataCollection(AdsdataTestCase):
    
    def test_last_synced(self):
        mongo = mongodb.get_mongo()
        self.assertTrue(BasicCollection.last_synced() is None, 'No previous DLT == last_synced() is None')
        
        now = datetime(2000,1,1)
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        mongo.insert(dlt)
        self.assertTrue(BasicCollection.last_synced() == now, 'last_synced() returns correct DLT')
        
    def test_last_modified(self):
        mongo = mongodb.get_mongo()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_COLLECTIONS['adsdata_test'] = tmp.name
        tmp_modified = datetime.fromtimestamp(os.stat(tmp.name)[ST_MTIME])
        last_modified = BasicCollection.last_modified()
        del config.MONGO_COLLECTIONS['adsdata_test']
        self.assertTrue(last_modified == tmp_modified, 'last_modfied() returns correct mod time')
        
    def test_needs_sync(self):
        mongo = mongodb.get_mongo()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_COLLECTIONS['adsdata_test'] = tmp.name
        self.assertTrue(BasicCollection.needs_sync(), 'No DLT == needs sync')
        
        sleep(1) 
        now = datetime.now()
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        mongo.insert(dlt)
        self.assertFalse(BasicCollection.needs_sync(), 'DLT sync time > file mod time == does not need sync')
        
        dlt.last_synced = now.replace(day=now.day - 1)
        mongo.update(dlt)
        self.assertTrue(BasicCollection.needs_sync(), 'DLT sync time < file mod time == needs sync')
        
    def test_load_data(self):
        mongo = mongodb.get_mongo()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_COLLECTIONS['adsdata_test'] = tmp.name
        for pair in zip("abcd","1234"):
            print >>tmp, "%s\t%s" % pair
        tmp.flush()
        self.assertTrue(BasicCollection.last_synced() is None)
        BasicCollection.load_data()
        self.assertTrue(type(BasicCollection.last_synced()) == datetime, 'load data creates DLT entry')
        self.assertEqual(mongo.query(BasicCollection).count(), 4, 'all records loaded')
        
    def test_load_data_aggregated(self):
        mongo = mongodb.get_mongo()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_COLLECTIONS['adsdata_test'] = tmp.name
        for pair in zip("aabbccdd","12345678"):
            print >>tmp, "%s\t%s" % pair
        tmp.flush()
        AggregatedCollection.load_data()
        self.assertEqual(mongo.query(AggregatedCollection).count(), 0, 'no records loaded in the actual collection')
        self.assertEqual(mongo.db['adsdata_test_load'].count(), 8, 'all records loaded in "_load" collection')
        
        utils.map_reduce_listify(mongo.db['adsdata_test_load'], 'adsdata_test', 'load_key', 'bar')
        self.assertEqual(mongo.query(AggregatedCollection).count(), 4, 'map-reduce loaded ')
        entry_a = mongo.query(AggregatedCollection).filter(AggregatedCollection.foo == 'a').first()
        self.assertTrue(entry_a is not None)
        self.assertEqual(entry_a.bar, ["1","2"])
    
if __name__ == '__main__':
    main()