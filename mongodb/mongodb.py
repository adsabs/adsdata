'''
Created on Oct 25, 2012

@author: jluker
'''

from config import config
from mongoalchemy.session import Session
from mongoalchemy.document import Document
from mongoalchemy.fields import *

mongo = None

def connect(config):
    return Session.connect(config.MONGO_DATABASE,
                safe=config.MONGO_SAFE,
                host=config.MONGO_HOST,
                port=config.MONGO_PORT)

def get_mongo(config=config):
    global mongo
    if mongo is None:
        mongo = connect(config)
    return mongo
    