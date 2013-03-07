'''
Created on Oct 25, 2012

@author: jluker
'''

import os
import sys
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) #@UndefinedVariable

import pytz
import tempfile
from stat import *
from time import sleep
from unittest import TestCase, main
from datetime import datetime, timedelta
from mongoalchemy import fields

from config import config
from adsdata import models, utils

class AdsdataTestCase(TestCase):
    
    def setUp(self):
        config.MONGO_DATABASE = 'test'
        config.MONGO_HOST = 'localhost'
        session = utils.get_session()
        session.drop_database('test')
    
    def tearDown(self):
        session = utils.get_session()
        session.drop_database('test')
    
class BasicCollection(models.DataCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.IntField()
    field_order = [foo, bar]
    
class NamedRestKeyCollection(models.DataCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.IntField()
    baz = fields.ListField(fields.StringField())
    restkey = 'baz'
    field_order = [foo, bar]
    
class AggregatedCollection(models.DataCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.ListField(fields.StringField())
    aggregated = True
    field_order = [foo, bar]
    
class TestDataCollection(AdsdataTestCase):
    
    def test_last_synced(self):
        session = utils.get_session()
        self.assertTrue(BasicCollection.last_synced(session) is None, 'No previous DLT == last_synced() is None')
        
        now = datetime(2000,1,1).replace(tzinfo=pytz.utc)
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        session.insert(dlt)
        self.assertTrue(BasicCollection.last_synced(session) == now, 'last_synced() returns correct DLT')
        
    def test_last_modified(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        tmp_modified = datetime.fromtimestamp(os.stat(tmp.name)[ST_MTIME]).replace(tzinfo=pytz.utc)
        last_modified = BasicCollection.last_modified()
        del config.MONGO_DATA_COLLECTIONS['adsdata_test']
        self.assertTrue(last_modified == tmp_modified, 'last_modfied() returns correct mod time')
        
    def test_needs_sync(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        self.assertTrue(BasicCollection.needs_sync(session), 'No DLT == needs sync')
        
        sleep(1) 
        now = datetime.now()
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        session.insert(dlt)
        self.assertFalse(BasicCollection.needs_sync(session), 'DLT sync time > file mod time == does not need sync')
        
        dlt.last_synced = now - timedelta(days=1)
        session.update(dlt)
        self.assertTrue(BasicCollection.needs_sync(session), 'DLT sync time < file mod time == needs sync')
        
    def test_load_data(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        for triplet in zip("abcd","1234","wxyz"):
            print >>tmp, "%s\t%s\t%s" % triplet
        tmp.flush()
        self.assertTrue(BasicCollection.last_synced(session) is None)
        BasicCollection.load_data(session)
        self.assertTrue(type(BasicCollection.last_synced(session)) == datetime, 'load data creates DLT entry')
        self.assertEqual(session.query(BasicCollection).count(), 4, 'all records loaded')
        
    def test_restkey(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        for triplet in zip("abcd","1234","wxyz"):
            print >>tmp, "%s\t%s\t%s" % triplet
        tmp.flush()
        BasicCollection.restkey = "unwanted"
        BasicCollection.load_data(session, source_file=tmp.name)
        entry_a = session.query(BasicCollection).filter(BasicCollection.foo == 'a').first()
        self.assertFalse(hasattr(entry_a, 'baz'))
        
    def test_named_restkey(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        for quad in zip("abcd","1234","wxyz", "5678"):
            print >>tmp, "%s\t%s\t%s\t%s" % quad
        tmp.flush()
        NamedRestKeyCollection.load_data(session, source_file=tmp.name)
        entry_a = session.query(NamedRestKeyCollection).filter(NamedRestKeyCollection.foo == 'a').first()
        self.assertEqual(entry_a.baz, ["w", "5"])
        
    def test_load_data_aggregated(self):
        session = utils.get_session()
        tmp = tempfile.NamedTemporaryFile()
        config.MONGO_DATA_COLLECTIONS['adsdata_test'] = tmp.name
        for pair in zip("aabbccdd","12345678"):
            print >>tmp, "%s\t%s" % pair
        tmp.flush()
        AggregatedCollection.load_data(session)
        self.assertEqual(session.query(AggregatedCollection).count(), 0, 'no records loaded in the actual collection')
        self.assertEqual(session.get_collection('adsdata_test_load').count(), 8, 'all records loaded in "_load" collection')
        
        utils.map_reduce_listify(session, session.get_collection('adsdata_test_load'), 'adsdata_test', 'load_key', 'bar')
        self.assertEqual(session.query(AggregatedCollection).count(), 4, 'map-reduce loaded ')
        entry_a = session.query(AggregatedCollection).filter(AggregatedCollection.foo == 'a').first()
        self.assertTrue(entry_a is not None)
        self.assertEqual(entry_a.bar, ["1","2"])
        
    def test_coerce_types(self):
        
        class CoerceCollection(models.DataCollection):
            foo = fields.StringField()
            bar = fields.IntField()
            
        class CoerceCollection2(models.DataCollection):
            foo = fields.FloatField()
            bar = fields.ListField(fields.StringField())
        
        class CoerceCollection3(models.DataCollection):
            foo = fields.ListField(fields.IntField())
            bar = fields.SetField(fields.FloatField())
            
        recs = [(BasicCollection, {"foo": "a", "bar": "3"}, {"foo": "a", "bar": 3}), # bar's str -> int
                (BasicCollection, {"foo": 1, "bar": "3"}, {"foo": 1, "bar": 3}), # foo's str ignored; bar's str -> int
                (BasicCollection, {"foo": "a", "bar": 3}, {"foo": "a", "bar": 3}), # bar's int preserved
                (AggregatedCollection, {"foo": "a", "bar": ["1","2"]}, {"foo": "a", "bar": ["1","2"]}), # bar's list of str ignored
                (CoerceCollection, {"foo": "a", "bar": "3"}, {"foo": "a", "bar": 3}), # bar's str -> int
                (CoerceCollection2, {"foo": "1.1234", "bar": ["a","b"]}, {"foo": 1.1234, "bar": ["a","b"]}), # foo's str -> float; bar's list of str ignored
                (CoerceCollection2, {"foo": "2", "bar": [1,2]}, {"foo": 2.0, "bar": [1,2]}), # foo's str > float; bar's list of int ignored
                (CoerceCollection3, {"foo": ["1", "2"], "bar": ["1.1234", "2"]}, {"foo": [1,2], "bar": [1.1234, 2.0]})
                ]
        for cls, rec, expected in recs:
            cls.coerce_types(rec)
            self.assertEqual(rec, expected)
        
    def test_generate_doc(self):
        pass
    
    def test_dt_manipulator(self):
        from adsdata.session import DatetimeInjector
        session = utils.get_session()
        session.add_manipulator(DatetimeInjector())
        collection = session.get_collection('ads_test')
        collection.insert({"foo": 1})
        entry = collection.find_one({"foo": 1}, manipulate=False)
        self.assertTrue(entry.has_key('_dt'))
        self.assertTrue(isinstance(entry['_dt'], datetime))
        # let the manipulator remove the _dt
        entry = collection.find_one({"foo": 1})
        self.assertFalse(entry.has_key('_dt'))
        
        # make sure that no '_dt' values are preserved
        dt = datetime.utcnow().replace(tzinfo=pytz.utc)
        collection.insert({"foo": 1, '_dt': dt})
        entry = collection.find_one({"foo": 1}, manipulate=False)
        self.assertNotEqual(dt, entry['_dt'])
        
    def test_digest_manipulator(self):
        from adsdata.session import DigestInjector
        session = utils.get_session()
        session.add_manipulator(DigestInjector())
        collection = session.get_collection('ads_test')
        collection.insert({"foo": 1})
        entry = collection.find_one({"foo": 1}, manipulate=False)
        self.assertTrue(entry.has_key('_digest'))
        
        digest = utils.doc_digest({"bar": 1})
        collection.insert({"baz": 1, "_digest": digest})
        entry = collection.find_one({"baz": 1}, manipulate=False)
        self.assertEqual(entry['_digest'], digest)
        
    def test_store_doc(self):
        pass
    
if __name__ == '__main__':
    main()