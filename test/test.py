'''
Created on Oct 25, 2012

@author: jluker
'''

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if sys.version_info < (2,7):
    import unittest2 as unittest
else:
    import unittest

import pytz
import tempfile
import mongobox

import subprocess
from stat import *
from time import sleep
from bson import DBRef
from datetime import datetime, timedelta
from mongoalchemy import fields
from mock import patch
from contextlib import contextmanager

from adsdata import models, utils
from adsdata.session import *
from adsdata.models import DataFileCollection

class BasicCollection(models.DataFileCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.IntField()
    field_order = [foo, bar]
    
class NamedRestKeyCollection(models.DataFileCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.IntField()
    baz = fields.ListField(fields.StringField())
    restkey = 'baz'
    field_order = [foo, bar]
    
class AggregatedCollection(models.DataFileCollection):
    config_collection_name = 'adsdata_test'
    foo = fields.StringField(_id=True)
    bar = fields.ListField(fields.StringField())
    aggregated = True
    field_order = [foo, bar]
    
    @classmethod
    def post_load_data(cls, *args, **kwargs):
        pass
    
def load_data(config):
    import subprocess
    test_data_dir = os.path.join(os.path.dirname(__file__), 'demo_data')
    for f in os.listdir(test_data_dir):
        abs_path = os.path.join(test_data_dir, f)
        collection_name = os.path.splitext(f)[0]
        with open(os.devnull, "w") as fnull:
            subprocess.call(["mongoimport", "--drop",
                             "-d", "test", 
                             "-c", collection_name, 
                             "-h", "%s:%d" % (config['ADSDATA_MONGO_HOST'], config['ADSDATA_MONGO_PORT']),
                             "-u", config['ADSDATA_MONGO_USER'],
                             "-p", config['ADSDATA_MONGO_PASSWORD'],
                             abs_path], stdout=fnull)       
            
class AdsdataTestCase(unittest.TestCase):
    
    def setUp(self):
        self.box = mongobox.MongoBox(scripting=True, auth=True)
        self.box.start()
        self.boxclient = self.box.client()
        self.boxclient['admin'].add_user('foo','bar')
        self.boxclient['admin'].authenticate('foo','bar')
        self.boxclient['test'].add_user('test','test')
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config = utils.load_config(os.path.join(base_dir, 'adsdata.cfg'))
        config['ADSDATA_MONGO_DATABASE'] = 'test'
        config['ADSDATA_MONGO_HOST'] = 'localhost'
        config['ADSDATA_MONGO_PORT'] = self.box.port
        config['ADSDATA_MONGO_USER'] = 'test'
        config['ADSDATA_MONGO_PASSWORD'] = 'test'
        self.config = config
        self.session = utils.get_session(config)
        load_data(self.config)
        
    def tearDown(self):
        self.box.stop()
    
class TestDataCollection(AdsdataTestCase):
    
    def test_last_synced(self):
        self.assertTrue(BasicCollection.last_synced(self.session) is None, 'No previous DLT == last_synced() is None')
        
        now = datetime(2000,1,1).replace(tzinfo=pytz.utc)
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        self.session.insert(dlt)
        self.assertTrue(BasicCollection.last_synced(self.session) == now, 'last_synced() returns correct DLT')
        
    def test_last_modified(self):
        tmp = tempfile.NamedTemporaryFile()
        tmp_modified = datetime.fromtimestamp(os.stat(tmp.name)[ST_MTIME]).replace(tzinfo=pytz.utc)
        last_modified = BasicCollection.last_modified(tmp.name)
        self.assertTrue(last_modified == tmp_modified, 'last_modfied() returns correct mod time')
        
    def test_needs_sync(self):
        tmp = tempfile.NamedTemporaryFile()
        self.assertTrue(BasicCollection.needs_sync(self.session, tmp.name), 'No DLT == needs sync')
        
        # sleep for a moment to ensure new last synced time is older than temp file
        sleep(0.1) 
        now = datetime.now()
        dlt = models.DataLoadTime(collection='adsdata_test', last_synced=now)
        self.session.insert(dlt)
        self.assertFalse(BasicCollection.needs_sync(self.session, tmp.name), 'DLT sync time > file mod time == does not need sync')
        
        dlt.last_synced = now - timedelta(days=1)
        self.session.update(dlt)
        self.assertTrue(BasicCollection.needs_sync(self.session, tmp.name), 'DLT sync time < file mod time == needs sync')
        
    def test_load_data(self):
        tmp = tempfile.NamedTemporaryFile()
        for pair in zip("abcd","1234"):
            print >>tmp, "%s\t%s" % pair
            
        # test a couple of lines with a missing values
        print >>tmp, "e"
        print >>tmp, "\t6"
        
        tmp.flush()
        self.assertTrue(BasicCollection.last_synced(self.session) is None)
        BasicCollection.load_data(self.session, tmp.name)
        self.assertTrue(type(BasicCollection.last_synced(self.session)) == datetime, 'load data creates DLT entry')
        self.assertEqual(self.session.query(BasicCollection).count(), 6, 'all records loaded')
        
        # check the rows with empty values
        collection = self.session.get_collection('adsdata_test')
        self.assertEqual(collection.find_one({'_id': 'e'}), {'_id': 'e', 'bar': ''})
        self.assertEqual(collection.find_one({'_id': ''}), {'_id': '', 'bar': 6})
        
    def test_load_data_post_load(self):
        
        tmp = tempfile.NamedTemporaryFile()
        for pair in zip("abcd","1234"):
            print >>tmp, "%s\t%s" % pair
        tmp.flush()
        
        with patch.object(DataFileCollection, 'post_load_data') as mock_post_load_data:
            BasicCollection.load_data(self.session, tmp.name)
            
        collection = self.session.get_collection('adsdata_test_load')
        self.assertEqual(collection.count(), 4)
        
    def test_restkey(self):
        tmp = tempfile.NamedTemporaryFile()
        for triplet in zip("abcd","1234","wxyz"):
            print >>tmp, "%s\t%s\t%s" % triplet
        tmp.flush()
        BasicCollection.restkey = "unwanted"
        BasicCollection.load_data(self.session, tmp.name)
        entry_a = self.session.query(BasicCollection).filter(BasicCollection.foo == 'a').first()
        self.assertFalse(hasattr(entry_a, 'baz'))
        
    def test_named_restkey(self):
        tmp = tempfile.NamedTemporaryFile()
        for quad in zip("abcd","1234","wxyz", "5678"):
            print >>tmp, "%s\t%s\t%s\t%s" % quad
        tmp.flush()
        NamedRestKeyCollection.load_data(self.session, tmp.name)
        entry_a = self.session.query(NamedRestKeyCollection).filter(NamedRestKeyCollection.foo == 'a').first()
        self.assertEqual(entry_a.baz, ["w", "5"])
    
    def test_load_data_aggregated(self):
        tmp = tempfile.NamedTemporaryFile()
        for pair in zip("aabbccdd","12345678"):
            print >>tmp, "%s\t%s" % pair
        tmp.flush()
        AggregatedCollection.load_data(self.session, tmp.name)
        
        utils.map_reduce_listify(self.session, self.session.get_collection('adsdata_test_load'), 'adsdata_test', 'load_key', 'bar')
        self.assertEqual(self.session.query(AggregatedCollection).count(), 4, 'map-reduce loaded ')
        entry_a = self.session.query(AggregatedCollection).filter(AggregatedCollection.foo == 'a').first()
        self.assertTrue(entry_a is not None)
        self.assertEqual(entry_a.bar, ["1","2"])
        
    def test_coerce_types(self):
        
        class CoerceCollection(models.DataFileCollection):
            foo = fields.StringField()
            bar = fields.IntField()
            
        class CoerceCollection2(models.DataFileCollection):
            foo = fields.FloatField()
            bar = fields.ListField(fields.StringField())
        
        class CoerceCollection3(models.DataFileCollection):
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

    def test_mapreduce_listify(self):
        source = self.session.get_collection("mapreduce_source")
        source.insert({"foo": "a", "bar": "z"})
        source.insert({"foo": "a", "bar": "y"})
        source.insert({"foo": "b", "bar": "z"})
        source.insert({"foo": "c", "bar": "z"})
        source.insert({"foo": "c", "bar": "x"})
        source.insert({"foo": "c", "bar": "w"})
        target = self.session.get_collection("mapreduce_target")
        utils.map_reduce_listify(self.session, source, target.name, "foo", "bar")
        self.assertEqual(target.count(), 3)
        self.assertEqual(target.find_one({"_id": "a"}), {"_id": "a", "bar": ["z","y"]})
        
    def test_load_data_duplicate_key(self):
        tmp = tempfile.NamedTemporaryFile()
        for pair in zip("abcdd","12345"):
            print >>tmp, "%s\t%s" % pair
            
        tmp.flush()
        BasicCollection.load_data(self.session, tmp.name)
        # duplicate record should quietly fail
        self.assertEqual(self.session.query(BasicCollection).count(), 4)
        
class TestDocs(AdsdataTestCase):        
    
    def test_generate_docs(self):
        load_data(self.config)
        self.maxDiff = None
        doc = self.session.generate_doc("1874MNRAS..34..279L")
        self.assertEqual(doc, {'ack': u'Lorem ipsum dolor sit amet, consecteteur adipiscing elit quisque, vel parturient.',
                               '_id': '1874MNRAS..34..279L',
                               'boost': 0.16849827679273299,
                               'citation_count': 0,
                               'full': u'Lorem ipsum dolor sit. Vitae ut aenean torquent feugiat in. Varius quis, condimentum blandit, donec sodales phasellus. Dignissim pellentesque parturient enim turpis dictum ipsum leo. Dolor ve amet sociosqu per, dapibus metus, eros ipsum. Curae taciti fames magna aptent eu ultricies. Vestibulum ut. Non nisl a. Malesuada nibh nec nisi sed imperdiet pulvinar, morbi et. Tortor laoreet nibh sollicitudin ac euismod pede, leo eget. Convallis morbi, ad semper in. Sapien. Phasellus nostra senectus curabitur lorem ad ve. Quam proin arcu quam cubilia feugiat sociis morbi fermentum. Imperdiet purus maecenas dui lectus nisi enim ut cras. At a urna id fringilla erat, viverra massa ad luctus sagittis. Nunc dignissim semper cursus, etiam integer sapien. Hymenaeos. Rhoncus lorem porta ante, eni elit molestie.',
                               'read_count': 4,
                               'norm_cites': 5,
                               'readers': [u'4f43e9286f', u'5108e7c0a8'],
                               'reads': [0, 0, 0, 0, 0, 0, 0, 5, 1, 1, 0, 1, 2, 0, 1, 0, 5, 3],
                               'refereed': True})
        doc = self.session.generate_doc("2011AJ....142...62H")
        self.assertEqual(doc, {'ack': None,
                               'grants': [{u'agency': u'NASA-HQ', u'grant': u'NNX09AF08G'}, {u'agency': u'NSF-AST', u'grant': u'0132798'}],
                               '_id': '2011AJ....142...62H',
                               'full': u'Lorem ipsum dolor sit amet, consecteteur. Montes purus nec. Eu habitasse euismod, tortor eros tortor sem, dictum, ut, faucibus. Justo. Lacinia augue integer dis id penatibus in, platea. Laoreet suspendisse nisl pede lobortis quis, eni augue nulla, pede fusce. Magna cursus penatibus. Eget nisl faucibus sed, orci praesent augue pellentesque, lacus in magnis nonummy. Ad. Id lacus ac. Bibendum hac, libero conubia mi, enim quam. Felis tempus dapibus rhoncus rutrum arcu nibh fames. Vehicula bibendum egestas sodales convallis quis dui malesuada montes. Taciti sem torquent habitasse pellentesque. Dapibus varius nunc eu fermentum a, eni tristique lorem velit. Curabitur egestas, magna in curabitur vestibulum. Lorem lacus. Vitae, mi ultricies imperdiet pede eget. Libero enim dolor.',
                               'readers': [u'430b0f6bd4', u'47d44dcaa9', u'48e27000f7', u'4cd02adfcc', u'4d46866c42', u'4d9b481763', u'4dce469f96', u'4f42520a18', u'4f63a3ac89', u'5039333cdb', u'504752fb6f', u'50844719d9', u'508fd5906b', u'50a267e8dd', u'50cf5b9972', u'50e4598eac', u'50e5930703', u'50ee1d6594', u'510ed9928c', u'51236f739c', u'51246644f5'],
                               'refereed': True})
        doc = self.session.generate_doc("1899Obs....22..253.")
        self.assertEqual(doc, {'ack': None,
                               '_id': '1899Obs....22..253.',
                               'boost': 0.115017967498934,
                               'citation_count': 0,
                               'full': u'Lorem ipsum dolor sit amet. Aenean rutrum fames condimentum orci mollis, etiam pretium libero erat nisl habitant libero, primis potenti, blandit. Rhoncus pellentesque tincidunt risus augue. Felis dui amet nunc. Nunc, amet natoque ac, molestie quisque, integer. Praesent vehicula curae sem diam sociis lacinia ac primis pretium in tellus suscipit consequat. Cursus, molestie ante taciti in. Diam massa in donec sem ante sed sed ut ante eros suscipit eni gravida aptent, justo ante, justo. Faucibus quam rhoncus pede, nisl dolor id tristique risus semper. Scelerisque sem montes at, senectus. Sapien neque rhoncus proin elit, aliquam fringilla tempor fringilla porta velit ullamcorper. Praesent nam, etiam tincidunt. Ullamcorper neque primis ut. Ad, ullamcorper pede, arcu duis vel eni sit. Nisl parturient porttitor class consequat. Dui elit id facilisis a vulputate risus, praesent etiam, ac ante duis magnis, et. Feugiat vel facilisis conubia, nulla. Ut dui quam.',
                               'read_count': 2,
                               'norm_cites': 11,
                               'reads': [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0],
                               'refereed': True})
        doc = self.session.generate_doc("1995MNRAS.274...31W")
        self.assertEqual(doc, {'ack': None,
                               '_id': '1995MNRAS.274...31W',
                               'full': u'Lorem ipsum dolor sit amet, consecteteur adipiscing elit cum adipiscing adipiscing. Fringilla, a donec ac sit elit elit nulla tempor pulvinar luctus arcu suspendisse cubilia curae parturient. Rhoncus, cum cras faucibus tincidunt amet nam nam curabitur sapien. Luctus quis convallis quam. Rutrum. Potenti ad, porttitor. A, rutrum arcu suspendisse conubia consectetuer vulputate litora adipiscing pellentesque. Turpis arcu, euismod eu, fringilla lorem et tortor. Sem pretium accumsan erat platea parturient morbi aliquet per. Vulputate enim eni. Sapien nisl sollicitudin sociosqu lorem. At sollicitudin posuere, sit. Potenti litora velit sociis at arcu metus in.',
                               'readers': [u'4f01774d0a', u'50effcf0d8', u'510ac1772a', u'51234f16c0', u'512d897d95'],
                               'refereed': True})
        doc = self.session.generate_doc("2002JPhA...35.8109K")
        self.assertEqual(doc, {'ack': None,
                               '_id': '2002JPhA...35.8109K',
                               'full': u'Lorem ipsum dolor sit amet. Pede sagittis per amet ut consectetuer. Sit donec lectus mauris ridiculus. Massa lectus porta, parturient dapibus. Libero eros, sollicitudin sagittis. Arcu consectetuer et aliquet ante, inceptos est euismod ut, congue dis. Egestas, vehicula eu turpis fringilla tortor varius, netus cursus nostra ipsum condimentum sit, tortor congue fringilla. Sapien molestie porttitor ve, ut mi sem vulputate. Ligula elit in nisl maecenas id, ornare mus conubia etiam. Netus nunc fames. Fusce. Turpis ligula interdum ullamcorper non semper. Ad nec hendrerit massa et. Varius pede nunc mi sollicitudin. Nonummy natoque odio dignissim curabitur enim sit odio nunc. Curae scelerisque quam ante curae scelerisque placerat metus. Quis. Tellus litora mus, et quam ipsum, proin orci congue phasellus. Nec odio elit viverra odio, lacus facilisis per. Egestas pellentesque.',
                               'readers': [u'X0cae078a6', u'X12049c5ae'],
                               'refereed': True})
        # simbad objects -- can't get it to run?
        doc = self.session.generate_doc("2012AJ....144...41M")
        self.assertEqual(doc, {'ack': None,
                               '_id': '2012AJ....144...41M',
                               'full': u'Lorem ipsum dolor sit amet, consecteteur adipiscing. Amet faucibus purus. Iaculis sollicitudin ornare id justo mi vitae taciti sociis nonummy. Ornare cursus magna per. Neque rhoncus sapien dictum nec, feugiat. Ut pulvinar quis. Potenti odio, consectetuer nascetur velit malesuada leo, sollicitudin luctus morbi ultricies. Proin pellentesque molestie lorem urna eni taciti a, quisque mollis. Tempus metus ullamcorper odio duis, elit curabitur torquent eu. Ridiculus. A. Suspendisse eu, cursus sociosqu. Lectus nec, euismod ve, nec. Montes, proin leo vitae pede metus, scelerisque eni, varius commodo.',
                               'readers': [ u"436f75a55a", u"46272c410e", u"490ab747dc", u"4939448062", u"4b5a08bbfe", u"4c6ba8aa3a", u"4c8b74d412", u"4e4e04ba43", u"4f062ccce1", u"501a251579", u"5027566670", u"50a4fe3774", u"50c080fcbf", u"50c3afbc0d", u"50f0d5b2b4", u"51008459eb", u"X058634054", u"X0655ebd3f", u"X0ae824e1a", u"X1b0df8aff", u"X1eef54acb", u"X21917fa62", u"X24bfec4a0", u"X25d68b840", u"X2a33f071f", u"X781029d71", u"X7da609b3b", u"X83d0b59d7", u"Xd118f4129", u"Xd727d89bb", u"Xe8ee1dc61", u"Xe8fe06b10" ],
                               'simbad_object_ids': [1514745, 1514748, 1514750, 1514751, 1514755, 1515364, 1519004, 1519087, 1519992, 1519997, 1520006, 1520008, 1520023, 1520024, 1520029, 1520032, 1520033, 1520034, 1520038, 1520139, 1520357, 1520371, 1520374, 1520381, 1520391, 1520395, 1520402, 1520404, 1520406, 1520413, 1520419, 1521360, 1521374, 1522778, 1575544, 3133169, 3754378, 5228155, 5228162, 5228174],
                               'refereed': True})
        
    def test_build_docs(self):
        load_data(self.config)
        self.session.store(self.session.generate_doc("2004PhRvD..70d6004F"), self.session.docs)
        doc = self.session.get_doc("2004PhRvD..70d6004F")
        self.assertTrue(doc['full'].startswith('Lorem ipsum dolor sit amet, consecteteur adipiscing elit lacinia.'))
        
        doc = self.session.generate_doc("2011AJ....142...62H")
        self.assertEqual(doc['refereed'], True)
    
    def test_dt_manipulator(self):
        self.session = utils.get_session(self.config, inc_manipulators=False)
        self.session.add_manipulator(DatetimeInjector('ads_test'))
        collection = self.session.get_collection('ads_test')
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
        self.session = utils.get_session(self.config, inc_manipulators=False)
        self.session.add_manipulator(DigestInjector('ads_test'))
        collection = self.session.get_collection('ads_test')
        collection.insert({"foo": 1})
        entry = collection.find_one({"foo": 1}, manipulate=False)
        self.assertTrue(entry.has_key('_digest'))
        
        digest = record_digest({"bar": 1}, self.session.db)
        collection.insert({"baz": 1, "_digest": digest})
        entry = collection.find_one({"baz": 1}, manipulate=False)
        self.assertEqual(entry['_digest'], digest)
        
    def test_dereference_manipulator(self):
        self.session = utils.get_session(self.config, inc_manipulators=False)
        collection_a = self.session.get_collection('test_a')
        collection_b = self.session.get_collection('test_b')
        collection_a.insert({"_id": 1, "foo": "bar"})
        collection_b.insert({"baz": "blah", "foo": DBRef(collection="test_a", id=1)})
        manipulator = DereferenceManipulator(ref_fields=[('test_b', 'foo')])
        self.session.add_manipulator(manipulator)
        doc = collection_b.find_one({"baz": "blah"})
        self.assertEqual(doc['foo'], 'bar')
        
    def test_fetch_doc(self):
        load_data(self.config)
        doc = self.session.get_doc("2012ASPC..461..837L")
        self.assertIsNotNone(doc)
        # _dt datestamp should be removed by manipulator
        self.assertNotIn("_dt", doc)
        
        # now get the doc again but retaining the _dt field
        doc = self.session.get_doc("2012ASPC..461..837L", manipulate=False)
        self.assertIn("_dt", doc)
        
    def test_store_doc(self):
        new_doc = {"_id": "2000abcd..123..456A", "foo": "bar"}
        digest = record_digest(new_doc, self.session.db)
        self.session.store(new_doc, self.session.docs)
        stored_doc = self.session.get_doc(new_doc['_id'], manipulate=False)
        self.assertIn("_digest", stored_doc)
        self.assertIn("_dt", stored_doc)
        self.assertEqual(stored_doc['_digest'], digest)
        
    def test_modify_existing_doc(self):
        load_data(self.config)
        existing_doc = self.session.get_doc("1999abcd.1234..111Q", manipulate=False)
        existing_digest = existing_doc['_digest']
        existing_dt = existing_doc['_dt']
        del existing_doc['_digest']
        del existing_doc['_dt']
        existing_doc['abcd'] = 1234
        new_digest = record_digest(existing_doc, self.session.db)
        self.session.store(existing_doc, self.session.docs)
        modified_doc = self.session.get_doc("1999abcd.1234..111Q", manipulate=False)
        self.assertEqual(modified_doc['_digest'], new_digest)
        self.assertNotEqual(modified_doc['_digest'], existing_digest)
        self.assertNotEqual(modified_doc['_dt'], existing_dt)
        
    def test_unmodified_existing_doc(self):
        existing_doc = self.session.get_doc("1999abcd.1234..111Q", manipulate=False)
        existing_digest = existing_doc['_digest']
        existing_dt = existing_doc['_dt']
        del existing_doc['_digest']
        del existing_doc['_dt']
        new_digest = record_digest(existing_doc, self.session.db)
        self.session.store(existing_doc, self.session.docs)
        unmodified_doc = self.session.get_doc("1999abcd.1234..111Q", manipulate=False)
        self.assertEqual(new_digest, unmodified_doc['_digest'])
        self.assertEqual(existing_digest, unmodified_doc['_digest'])
        # datetime value should not have been updated
        self.assertEqual(existing_dt, unmodified_doc['_dt'])
        
class TestMetrics(AdsdataTestCase):        
    
    def test_generate_metrics_data(self):
        load_data(self.config)
        self.maxDiff = None
        doc = self.session.generate_metrics_data("1920ApJ....51....4D")
        self.assertEqual(doc, {'_id': '1920ApJ....51....4D',
                               'refereed': True,
                               'rn_citations': 0.070302403721891962,
                               'downloads': [0, 0, 0, 5, 3, 3, 2, 6, 1, 8, 7, 2, 7, 3, 2, 0, 4, 5],
                               'reads': [0, 0, 0, 5, 4, 3, 3, 6, 1, 8, 12, 4, 7, 3, 2, 2, 8, 0],
                               'an_citations': 0.052631578947368418,
                               'refereed_citation_num': 4,
                               'citation_num': 5,
                               'citations': [u'1983ARA&A..21..373O', u'2000JOptB...2..534W', u'2000PhRvL..84.2094A', u'2001AJ....122..308G', u'2011foobar........X'],
                               'refereed_citations': [u'1983ARA&A..21..373O', u'2000JOptB...2..534W', u'2000PhRvL..84.2094A', u'2001AJ....122..308G'],
                               'author_num': 1,
                               'an_refereed_citations': 0.042105263157894736
                               })
    def test_build_metrics_data(self):
        load_data(self.config)
        self.session.store(self.session.generate_metrics_data("1920ApJ....51....4D"), self.session.metrics_data)
        doc = self.session.get_metrics_data("1920ApJ....51....4D")
        self.assertEqual(doc['citation_num'], 5)
        self.assertEqual(doc['refereed_citation_num'], 4)
        self.assertEqual(doc['refereed'], True)

if __name__ == '__main__':
    unittest.main()
