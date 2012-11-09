'''
Created on Oct 25, 2012

@author: jluker
'''

from config import config
from collections import defaultdict
from hashlib import sha1
from mongoalchemy.session import Session
from mongoalchemy.document import Document
from mongoalchemy.document import Index
from mongoalchemy.fields import *

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
    