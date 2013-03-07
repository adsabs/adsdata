'''
Created on Oct 25, 2012

@author: jluker
'''

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pymongo") 

import pytz
from datetime import datetime
from pymongo.son_manipulator import SONManipulator
from mongoalchemy.session import Session

from adsdata import utils

class DatetimeInjector(SONManipulator):
    """
    Used for injecting/removing the datetime values of records in
    the docs collection
    """
    def transform_incoming(self, son, collection):
        son['_dt'] = datetime.utcnow().replace(tzinfo=pytz.utc)
        return son
    
    def transform_outgoing(self, son, collecction):
        if son.has_key('_dt'):
            del son['_dt']
        return son
    
class DigestInjector(SONManipulator):
    
    def transform_incoming(self, son, collection):
        if not son.has_key('_digest'):
            son['_digest'] = utils.doc_digest(son)
        return son

class DataSession(object):
    """
    Wraps a mongoalchemy.Session object and provides methods for 
    directly accessing the internal pymongo client and for querying
    the data collections in models.py
    """
    def __init__(self, db, uri, safe=False, create_ok=False):
        
        self.malchemy = Session.connect(db, host=uri, timezone=pytz.utc)
        self.create_ok = create_ok
        self._db = self.malchemy.db
        self._pymongo = self._db.connection
        if safe:
            self._pymongo.db.write_concern = {'w': 1, 'j': True}
    
    def add_manipulator(self, manipulator):
        self._db.add_son_manipulator(manipulator)
        
    def drop_database(self, database_name):
        self._pymongo.drop_database(database_name)
        
    def get_collection(self, collection_name):
        return self._db[collection_name]
    
    def query(self, *args, **kwargs):
        return self.malchemy.query(*args, **kwargs)
    
    def insert(self, *args, **kwargs):
        return self.malchemy.insert(*args, **kwargs)
    
    def update(self, *args, **kwargs):
        return self.malchemy.update(*args, **kwargs)
    
    def iterate(self, model):
        q = self.malchemy.query(model)
        return self.malchemy.execute_query(q, self.malchemy)
    
    def docs_sources(self):
        if not hasattr(self, 'doc_source_models'):
            from adsdata.models import doc_source_models
            self.doc_source_models = doc_source_models()
        return self.doc_source_models
    
    def generate_doc(self, bibcode):
        doc = {'bibcode': bibcode}
        for model_class in self.docs_sources():
            model_class.add_docs_data(doc, self, bibcode)
        return doc
                
    def store_doc(self, collection_name, doc):
        
        # note: bypassing mongoalchemy here
        collection = self.get_collection(collection_name)
        
        digest = utils.doc_digest(doc)
        spec = {'bibcode': doc['bibcode'], '_digest': digest}
        
        # look for existing doc with the same digest value
        existing = collection.find_one(spec)
        if existing:
            # no change so do nothing.
            return
        
        # save doc with the new digest value
        # note: spec still contains old digest as a precaution against a
        # race condition where we clobber the update from a diff process
        doc['_digest'] = digest
        return collection.update(spec, doc, manipulate=True, upsert=True)
            