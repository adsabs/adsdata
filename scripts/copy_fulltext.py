'''
Created on Mar 12, 2013

@author: jluker

This script is intended as a transitional process that will copy fulltext content
from the old fulltext pipeline mongo instance on adszee to the new adsdata
mongo instance on adsx.
'''
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import time
import logging
import itertools
from multiprocessing import Process, Queue, cpu_count
from datetime import timedelta,datetime
from optparse import OptionParser
from pymongo import MongoClient

from adsdata import utils, models

def uri2collection(uri):
    host, db, collection = uri.split("/")
    mongo = MongoClient(host)
    return mongo[db][collection]
    
class Copier(Process):

    def __init__(self, task_queue, opts, config):
        Process.__init__(self)
        self.task_queue = task_queue
        self.from_collection = MongoClient(host=config['ADSZEE_MONGO_URI'])['solr4ads']['docs']
        session = utils.get_session(config)
        self.to_collection = session.get_collection('fulltext')
        self.wanted = dict([(x,1) for x in opts.fields.split(',')])

    def run(self):
        log = logging.getLogger()
        while True:
            bib = self.task_queue.get()
            if bib is None:
                log.info("Nothing left to do for worker %s" % self.name)
                break
            log.info("Copier %s is working on %s" % (self.name, bib))
            try:
                log.info("Copier %s is querying on %s" % (self.name, bib))
                doc = self.from_collection.find_one({'bibcode': bib}, self.wanted)
                if not doc: continue
                doc['_id'] = bib
                del doc['bibcode']
                self.to_collection.update({'_id': bib}, doc, upsert=True)
            except:
                raise

def main(opts, config):
    
    log = logging.getLogger()
    
    session = utils.get_session(config)
    
    bibiter = session.iterate(models.FulltextLink)
    bibiter = itertools.imap(lambda x: x.bibcode, bibiter)
    if opts.limit:
        bibiter = itertools.islice(bibiter, opts.limit)
    
    tasks = Queue()

    # start up our builder threads
    log.debug("Creating %d Copier processes" % opts.threads)
    procs = [ Copier(tasks, opts, config) for i in xrange(opts.threads)]
    for p in procs:
        p.start()

    for bib in bibiter:
        tasks.put(bib)
        
    # add some poison pills to the end of the queue
    log.debug("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)

    log.info("All work complete")
    
if __name__ == '__main__':
    
    op = OptionParser()
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=8)#cpu_count())# * 2)
    op.add_option('--limit', dest='limit', action='store',
        help='process this many', type=int, default=None)
    op.add_option('--fields', dest='fields', action='store',
        help='comma-separated list of mongo fields that will be used', type=str, default="bibcode,full,ack")
    op.add_option('--gtime', dest='gtime', action='store',
        help='index items last generated less than this many d/h/m ago', type=str)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config = utils.load_config(os.path.join(base_dir, 'adsdata.cfg'))

    log = utils.init_logging(base_dir, __file__, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)
    
    start_cpu = time.clock()
    start_real = time.time()        
    
    main(opts, config)
    
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)
