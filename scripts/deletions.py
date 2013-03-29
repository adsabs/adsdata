'''
Created on Mar 12, 2013

@author: jluker
'''
import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
import itertools
from Queue import Empty as QueueEmpty
from multiprocessing import Process, JoinableQueue, Queue, cpu_count
from optparse import OptionParser
from pymongo import MongoClient

from config import config
from adsdata import utils, models

def uri2collection(uri):
    host, db, collection = uri.split("/")
    mongo = MongoClient(host)
    return mongo[db][collection]
    
class Worker(Process):

    def __init__(self, task_queue, deletes_queue, authority):
        Process.__init__(self)
        self.task_queue = task_queue
        self.deletes_queue = deletes_queue
        session = utils.get_session()
        self.authority_collection = session.get_collection(authority)

    def run(self):
        log = logging.getLogger()
        while True:
            bib = self.task_queue.get()
            if bib is None:
                log.debug("Nothing left to do for worker %s" % self.name)
                self.task_queue.task_done()
                log.info("task queue size: %d" % self.task_queue.qsize())
                break
            log.debug("Worker %s is working on %s" % (self.name, bib))
            try:
                doc = self.authority_collection.find_one({'_id': bib}, {'_id': 1})
                if doc:
                    log.debug("%s appears in authority collection" % bib)
                else:
                    log.debug("%s is missing from authority collection" % bib)
                    self.deletes_queue.put(bib)
            except:
                raise
            finally:
                self.task_queue.task_done()
                log.info("task queue size: %d" % self.task_queue.qsize())

def find_deletions(opts):
    
    log = logging.getLogger()
    
    session = utils.get_session()
    subject_collection = session.get_collection(opts.subject)
    bibiter = itertools.imap(lambda x: x['_id'], subject_collection.find({}, {'_id': 1}))
    
    if opts.limit:
        bibiter = itertools.islice(bibiter, opts.limit)
    
    tasks = JoinableQueue()
    deletes = Queue()

    # start up our builder threads
    log.debug("Creating %d Worker processes" % opts.threads)
    procs = [ Worker(tasks, deletes, opts.authority) for i in xrange(opts.threads)]
    for p in procs:
        p.start()

    log.debug("Queueing work")
    count = 0
    for count, bib in enumerate(bibiter, 1):
        tasks.put(bib)
        
    log.info("Subject collection contained %d items" % count)
        
    # add some poison pills to the end of the queue
    log.debug("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)
        
    log.debug("joining task queue")
    tasks.join()
    
    while True:
        try:
            yield deletes.get_nowait()
        except QueueEmpty:
            break

def delete(opts):
    session = utils.get_session()
    subject_collection = session.get_collection(opts.subject)
    count = 0
    for count, bib in enumerate(find_deletions(opts), 1):
        log.info("deleting %s" % bib)
        subject_collection.remove({'_id': bib})
    log.info("done. %d items deleted" % count)

def list(opts):
    for bib in find_deletions(opts):
        print bib

if __name__ == '__main__':
    
    commands = ['list','delete']
    
    op = OptionParser()
    op.add_option('-b', '--database', dest="database", action="store", default='adsdata',
                  help="database to connect to")
    op.add_option('-a', '--authority', dest="authority", action="store", default=models.Accno.config_collection_name,
                  help="collection to use as the authority")
    op.add_option('-s', '--subject', dest="subject", action="store", default='docs',
                  help="collection to be examined for possible deletions")
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=cpu_count())# * 2)
    op.add_option('-l','--limit', dest='limit', action='store',
        help='process this many', type=int, default=None)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    logfile = "%s/%s" % (config.LOG_DIR, os.path.basename(__file__))
    log = utils.init_logging(logfile, opts.verbose, opts.debug)
    
    try:
        cmd = args.pop()
        assert cmd in commands
    except (IndexError,AssertionError):
        op.error("missing or invalid command")    
        
    start_cpu = time.clock()
    start_real = time.time()        
    
    eval(cmd)(opts)
    
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)
