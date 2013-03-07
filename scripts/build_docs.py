'''
Created on Feb 28, 2013

@author: jluker
'''

import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
import time
import logging
import itertools
from datetime import datetime
from optparse import OptionParser
from Queue import Empty as QueueEmpty
from multiprocessing import Process, Queue, JoinableQueue, cpu_count

from adsdata import utils
from adsdata.session import DatetimeInjector
from adsdata.models import Accno
from adsdata.exceptions import *
from config import config

class Builder(Process):
    
    def __init__(self, task_queue, result_queue):
        Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.session = utils.get_session()
        
    def run(self):
        log = logging.getLogger()
        while True:
            bibcode = self.task_queue.get()
            if bibcode is None:
                log.info("Nothing left to build for worker %s" % self.name)
                break
            log.info("Worker %s: working on %s" % (self.name, bibcode))
            try:
                result = self.session.generate_doc(bibcode)
                self.result_queue.put(result)
            except DocDataException, e:
                log.error("Something went wrong building %s: %s" % (bibcode, e))
            except:
                log.error("Something went wrong building %s" % bibcode)
                raise
            finally:
                self.task_queue.task_done()
        return

class Saver(Process):
    
    def __init__(self, result_queue, collection_name):
        Process.__init__(self)
        self.result_queue = result_queue
        self.session = utils.get_session()
        self.collection_name = collection_name
        self.session.add_manipulator(DatetimeInjector())
        
    def run(self):
        log = logging.getLogger()
        while True:
            doc = self.result_queue.get()
            if doc is None:
                log.info("Nothing left to save for worker %s" % self.name)
                break
            log.info("Saver %s is working on %s" % (self.name, doc['bibcode']))
            try:
                saved = self.session.store_doc(self.collection_name, doc)
                log.info("saved: %s" % str(saved))
            except:
                raise
        
def get_bibcodes(opts):
    
    if opts.infile:
        if opts.infile == '-':
            stream = sys.stdin
        else:
            stream = open(opts.infile, 'r')
        bibcodes = itertools.imap(lambda x: x.strip(), stream)
    else:
        session = utils.get_session()
        bibcodes = itertools.imap(lambda x: x.bibcode, session.iterate(Accno))
        
    if opts.limit:
        bibcodes = itertools.islice(bibcodes, opts.limit)
    
    return bibcodes
    
def build_synchronous(opts):
    session = utils.get_session()
    session.add_manipulator(DatetimeInjector())
    for bib in get_bibcodes(opts):
        doc = session.generate_doc(bib)
        if doc is not None:
            saved = session.store_doc(opts.collection, doc)
            log.info("Saved: %s" % str(saved))
    return
        
def build(opts):
    tasks = JoinableQueue()
    results = Queue()
    
    # start up our builder threads
    log.info("Creating %d Builder processes" % opts.threads)
    builders = [ Builder(tasks, results) for i in xrange(opts.threads)]
    for b in builders:
        b.start()
        
    log.info("Creating %d Saver processes" % opts.threads)
    savers = [ Saver(results, opts.collection) for i in xrange(opts.threads)]
    for s in savers:
        s.start()
        
    # queue up the bibcodes
    for bib in get_bibcodes(opts):
        tasks.put(bib)
    
    # add some poison pills to the end of the queue
    for i in xrange(opts.threads):
        tasks.put(None)
    
    # block until all tasks in the task queue are completed
    tasks.join()
    
    # poison our saver threads
    for i in xrange(opts.threads):
        results.put(None)
    
    while True:
        try:
            doc = results.get(True, 3)
            log.info("Result: %s" % str(doc))
        except QueueEmpty:
            break

def status(opts):
    pass

if __name__ == "__main__":
    
    commands = ['build','build_synchronous', 'status']
    
    op = OptionParser()
    op.set_usage("usage: sync_mongo_data.py [options] [%s]" % '|'.join(commands))
    op.add_option('-c', '--collection', dest="collection", action="store", default=config.MONGO_DOCS_COLLECTION)
    op.add_option('-i', '--infile', dest="infile", action="store")
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=cpu_count() * 2)
    op.add_option('-l','--limit', dest="limit", action="store", type=int)
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