'''
Created on Oct 25, 2012

@author: jluker
'''

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pymongo") 
warnings.filterwarnings("ignore", category=DeprecationWarning, module="mongoalchemy") 

import pytz
import logging
import hashlib
from json import dumps
from bson import DBRef
from datetime import datetime
from mongoalchemy.session import Session
from pymongo.son_manipulator import SONManipulator

DOCS_COLLECTION = 'docs'
METRICS_DATA_COLLECTION = 'metrics_data'
MONGO_DOCS_DEREF_FIELDS = []

class DataSession(object):
    """
    Wraps a mongoalchemy.Session object and provides methods for 
    directly accessing the internal pymongo client and for querying
    the data collections in models.py
    """
    def __init__(self, db, uri, create_ok=False, inc_manipulators=True):
        
        self.malchemy = Session.connect(db, host=uri, timezone=pytz.utc)
        self.create_ok = create_ok
        self.db = self.malchemy.db
        self.docs = self.db[DOCS_COLLECTION]
        self.docs.ensure_index('_digest')
        self.metrics_data = self.db[METRICS_DATA_COLLECTION]
        self.metrics_data.ensure_index('_digest')
        self.pymongo = self.db.connection
        if inc_manipulators:
            # NOTE: order is important here
            self.add_manipulator(DigestInjector([DOCS_COLLECTION, METRICS_DATA_COLLECTION]))
            self.add_manipulator(DatetimeInjector([DOCS_COLLECTION, METRICS_DATA_COLLECTION]))
            self.add_manipulator(DereferenceManipulator(MONGO_DOCS_DEREF_FIELDS))
    
    def add_manipulator(self, manipulator):
        self.db.add_son_manipulator(manipulator)
        
    def drop_database(self, database_name):
        self.pymongo.drop_database(database_name)
        
    def get_collection(self, collection_name):
        return self.db[collection_name]
    
    def query(self, *args, **kwargs):
        return self.malchemy.query(*args, **kwargs)
    
    def insert(self, *args, **kwargs):
        return self.malchemy.insert(*args, **kwargs)
    
    def update(self, *args, **kwargs):
        return self.malchemy.update(*args, **kwargs)
    
    def iterate(self, model):
        q = self.malchemy.query(model)
        return self.malchemy.execute_query(q, self.malchemy)
    
    def get_doc(self, bibcode, manipulate=True):
        spec = {'_id': bibcode}
        return self.docs.find_one(spec, manipulate=manipulate)
        
    def docs_sources(self):
        if not hasattr(self, 'doc_source_models'):
            from adsdata.models import doc_source_models
            self.doc_source_models = list(doc_source_models())
        return self.doc_source_models
    
    def generate_doc(self, bibcode):
        doc = {'_id': bibcode}
        for model_class in self.docs_sources():
            model_class.add_docs_data(doc, self, bibcode)
        return doc

    def get_metrics_data(self, bibcode, manipulate=True):
        spec = {'_id': bibcode}
        return self.metrics_data.find_one(spec, manipulate=manipulate)

    def metrics_data_sources(self):
        if not hasattr(self, 'metrics_data_source_models'):
            from adsdata.models import metrics_data_source_models
            self.metrics_data_source_models = list(metrics_data_source_models())
        return self.metrics_data_source_models

    def generate_metrics_data(self, bibcode):
        doc = {'_id': bibcode}
        for model_class in self.metrics_data_sources():
            model_class.add_metrics_data(doc, self, bibcode)
        return doc

    def store(self, record, collection):
        
        log = logging.getLogger()
        
        record["_digest"] = record_digest(record, self.db)
        spec = {'_id': record['_id'] } #, '_digest': digest}
        
        # look for existing doc; fetch only id & _digest values
        existing = collection.find_one(spec, { "_digest": 1 }, manipulate=False)
        if existing:
            # do the digest values match?
            if existing.has_key("_digest") and existing["_digest"] == record["_digest"]:
                # no change; do nothing
                log.debug("Digest match. No change to %s", str(spec))
                return
            elif existing.has_key("_digest"):
                # add existing digest value to spec to avoid race conditions
                spec['_digest'] = existing["_digest"]
        
        # NOTE: even for cases where there was no existing doc we need to do an 
        # upsert to avoid race conditions
        return collection.update(spec, record, manipulate=True, upsert=True)

class DatetimeInjector(SONManipulator):
    """
    Used for injecting/removing the datetime values of records in
    the docs collection
    """
    def __init__(self, collections=[]):
        self.collections = collections
        
    def transform_incoming(self, son, collection):
        if collection.name in self.collections:
            son['_dt'] = datetime.utcnow().replace(tzinfo=pytz.utc)
        return son
    
    def transform_outgoing(self, son, collection):
        if collection.name in self.collections:
            if son.has_key('_dt'):
                del son['_dt']
        return son
    
class DigestInjector(SONManipulator):
    """
    Inserts a digest hash of the contents of the doc being inserted
    """
    def __init__(self, collections=[]):
        self.collections = collections
        
    def transform_incoming(self, son, collection):
        if collection.name in self.collections:
            if not son.has_key('_digest'):
                son['_digest'] = record_digest(son, collection.database)
        return son
    
    def transform_outgoing(self, son, collection):
        if collection.name in self.collections:
            if son.has_key('_digest'):
                del son['_digest']
        return son 
    
class DereferenceManipulator(SONManipulator):
    """
    Automatically de-references DBRef links to other docs
    """
    def __init__(self, ref_fields=[]):
        """
        ref_fields - a list of tuples in the form (collection, fieldname)
        where collection is the name of a collection to work on and fieldname
        is the document field that contains the DBRef values
        """
        self.ref_fields = {}
        for collection, field in ref_fields:
            self.ref_fields.setdefault(collection, [])
            self.ref_fields[collection].append(field)
        
    def transform_outgoing(self, son, collection):
        if collection.name in self.ref_fields:
            for field_name in self.ref_fields[collection.name]:
                dereference(son, collection.database, field_name)
        return son
    
def record_digest(record, db, hashtype='sha1'):
    """
    generate a digest hash from a 'docs' dictionary
    """
    # first make a copy
    digest_record = record.copy()
    
    # remove any 'meta' values
    for k in digest_record.keys():
        if k.startswith('_'):
            del digest_record[k]
        elif isinstance(digest_record[k], DBRef):
            dereference(digest_record, db, k)
            
    h = hashlib.new(hashtype)
    # sort_keys=True should make this deterministic?
    json = dumps(digest_record, sort_keys=True)
    h.update(json)
    return h.hexdigest()                

def dereference(son, db, field_name):
    """
    convert the DBRef value in 'field_name' to it's dereferenced value
    """
    db_ref = son.get(field_name)
    if not isinstance(db_ref, DBRef):
        return
    ref_doc = db.dereference(db_ref)
    son[field_name] = ref_doc.get(field_name)
    
