'''
Created on Oct 25, 2012

@author: jluker
'''

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pymongo") 

import sys
import pymongo
from collections import defaultdict
from hashlib import sha1
from mongoalchemy.session import Session
from mongoalchemy.document import Document
from mongoalchemy.document import Index
from mongoalchemy.fields import *

from config import config

mongo = None

def _init(config):
    mongo = Session.connect(config.MONGO_DATABASE,
                safe=config.MONGO_SAFE,
                host=config.MONGO_HOST,
                port=config.MONGO_PORT)
    return mongo

def get_mongo(config=config):
    global mongo
    if mongo is None:
        mongo = _init(config)
    return mongo
    
class DataClient(object):
    """
    Wraps a pymongo.MongoClient object and provides convenience methods for
    querying the data collections
    """
    def __init__(self, host=config.MONGO_HOST, port=config.MONGO_PORT,
             safe=True, db=config.MONGO_DATABASE, create_ok=False):
        
        try:
            self.client = pymongo.MongoClient(host, port)
            if safe:
                # TODO: maybe expose this in a more granular way
                self.client.write_concern = {'w': 1, 'j': True}
        except pymongo.errors.ConnectionFailure, e:
            raise Exception("Client failed to connect: %s" % str(e))
        
        if db not in self.client.database_names() and not create_ok:
            raise Exception("Database doesn't exist. Pass create_ok=True to create it.")
            
            