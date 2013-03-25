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
import itertools
from multiprocessing import Process, Queue, cpu_count
from datetime import timedelta,datetime
from optparse import OptionParser
from pymongo import MongoClient

from config import config
from adsdata import utils,models

def uri2collection(uri):
    host, db, collection = uri.split("/")
    mongo = MongoClient(host)
    return mongo[db][collection]
    
class Copier(Process):

    def __init__(self, task_queue, opts):
        Process.__init__(self)
        self.task_queue = task_queue
        self.remote = MongoClient(host='mongodb://adszee:27017')['solr4ads']['docs']
        local_db = MongoClient(host='mongodb://localhost:27017')['adsdata']
        local_db.authenticate('adsdata','Ri1cGV7sNr')
        self.local = local_db['fulltext']
        self.query = {}
        self.wanted = dict([(x,1) for x in opts.fields.split(',')])
        if opts.gtime:
            (n, unit) = re.search('^(\d+)(d|h|m)$', opts.gtime).groups()
            if unit == 'd':
                tdelta = timedelta(int(n))
            elif unit == 'h':
                tdelta = timedelta(0, 0, 0, 0, 0, int(n))
            elif unit == 'm':
                tdelta = timedelta(0, 0, 0, 0, int(n))
            self.query['_generated'] = {"$gt": datetime.now() - tdelta}

    def run(self):
        log = logging.getLogger()
        while True:
            bib = self.task_queue.get()
            if bib is None:
                log.info("Nothing left to do for worker %s" % self.name)
                break
            log.info("Copier %s is working on %s" % (self.name, bib))
            self.query['bibcode'] = bib
            try:
                log.info("Copier %s is querying on %s" % (self.name, bib))
                doc = self.remote.find_one(self.query, self.wanted)
                if not doc: continue
                doc['_id'] = bib
                del doc['bibcode']
                self.local.save(doc)
            except:
                raise

def main(opts):
    
    log = logging.getLogger()
    
    session = utils.get_session()
    to_collection = session.get_collection('fulltext')
    to_collection.drop()
    
    bibiter = session.iterate(models.FulltextLink)
    bibiter = itertools.imap(lambda x: x.bibcode, bibiter)
    if opts.limit:
        bibiter = itertools.islice(bibiter, opts.limit)
    
    tasks = Queue()

    # start up our builder threads
    log.info("Creating %d Builder processes" % opts.threads)
    procs = [ Copier(tasks, opts) for i in xrange(opts.threads)]
    for p in procs:
        p.start()

    for bib in bibiter:
        tasks.put(bib)
        
    # add some poison pills to the end of the queue
    log.info("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)

    log.info("All work complete")
    
if __name__ == '__main__':
    
    op = OptionParser()
    op.add_option('--from_mongo', dest="from_mongo", action="store")
    op.add_option('--to_mongo', dest="to_mongo", action="store")
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=8)#cpu_count())# * 2)
    op.add_option('--limit', dest='limit', action='store',
        help='process this many', type=int, default=None)
#    op.add_option('--query', dest='query', action='store',
#        help='documents that match this mongodb query will be indexed', type=str, default="{'ft_type': {'$exists': True}}")
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
