'''
Created on Oct 25, 2012

@author: jluker
'''

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pymongo") 

import sys
import pytz
from mongoalchemy.session import Session

from config import config

session = None

def get_session(config=config):
    global session
    if session is None:
        session = DataSession(config.MONGO_DATABASE)
    return session
    
class DataSession(object):
    """
    Wraps a mongoalchemy.Session object and provides methods for 
    directly accessing the internal pymongo client and for querying
    the data collections in models.py
    """
    def __init__(self, db, safe=config.MONGO_SAFE, host=config.MONGO_HOST, 
                 port=config.MONGO_PORT, create_ok=False):
        
        self.session = Session.connect(db, host=host, port=port, timezone=pytz.utc)
        self._db = self.session.db
        self._pymongo = self._db.connection
        if safe:
            self._pymongo.db.write_concern = {'w': 1, 'j': True}
    
    def get_collection(self, collection_name):
        return self._db[collection_name]
    
    def query(self, type):
        return self.session.query(type)
    
    def iterate(self, type):
        q = self.session.query(type)
        return self.session.execute_query(q, self.session)
    
    def docs_sources(self):
        if not hasattr(self, 'doc_source_models'):
            from adsdata.models import doc_source_models
            self.doc_source_models = doc_source_models()
        return self.doc_source_models
    
    def generate_doc(self, bibcode):
        doc = {'bibcode': bibcode}
        for model_class in self.docs_sources():
            model_class.add_docs_data(doc, self, bibcode)
#            
#            for field in model_class.docs_fields:
#                key = field.db_field
#                value = self.session.query(model_class).filter(model_class.bibcode == bibcode).first()
#                data[key] = value
        return doc
                
            
            