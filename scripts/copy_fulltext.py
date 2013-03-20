'''
Created on Mar 12, 2013

@author: jluker
'''
import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import time
import logging
from datetime import timedelta,datetime
from optparse import OptionParser
from pymongo import MongoClient

from config import config
from adsdata import utils

def uri2collection(uri):
    host, db, collection = uri.split("/")
    mongo = MongoClient(host)
    return mongo[db][collection]
    
def main(opts):
    
    log = logging.getLogger()
    
    from_collection = uri2collection(opts.from_mongo)
    to_collection = uri2collection(opts.to_mongo)
    to_collection.drop()
    
    query = eval(opts.query)

    if opts.gtime:
        (n, unit) = re.search('^(\d+)(d|h|m)$', opts.gtime).groups()
        if unit == 'd':
            tdelta = timedelta(int(n))
        elif unit == 'h':
            tdelta = timedelta(0, 0, 0, 0, 0, int(n))
        elif unit == 'm':
            tdelta = timedelta(0, 0, 0, 0, int(n))

        query['_generated'] = {"$gt": datetime.now() - tdelta}
        
    wanted = dict([(x,1) for x in opts.fields.split(',')])
    log.info("fields: %s" % str(wanted))
        
    log.info("using query: %s" % query)
    cursor = from_collection.find(query, wanted).limit(opts.limit)
    
    for ft in cursor:
        log.info("copying %s" % ft['bibcode'])
        ft['_id'] = ft['bibcode']
        del ft['bibcode']
        to_collection.insert(ft)
        
    log.info("done")
    
if __name__ == '__main__':
    
    op = OptionParser()
    op.add_option('--from_mongo', dest="from_mongo", action="store")
    op.add_option('--to_mongo', dest="to_mongo", action="store")
    op.add_option('--limit', dest='limit', action='store',
        help='process this many', type=int, default=0)
    op.add_option('--query', dest='query', action='store',
        help='documents that match this mongodb query will be indexed', type=str, default="{'ft_type': {'$exists': True}}")
    op.add_option('--fields', dest='fields', action='store',
        help='comma-separated list of mongo fields that will be used', type=str, default="bibcode,full,ack")
    op.add_option('--gtime', dest='gtime', action='store',
        help='index items last generated less than this many d/h/m ago', type=str)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    logfile = "%s/%s" % (config.LOG_DIR, os.path.basename(__file__))
    log = utils.init_logging(logfile, opts.verbose, opts.debug)
    
    start_cpu = time.clock()
    start_real = time.time()        
    
    main(opts)
    
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)
