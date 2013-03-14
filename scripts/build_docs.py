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
from adsdata import models
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
                self.task_queue.task_done()
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
    
    def __init__(self, result_queue):
        Process.__init__(self)
        self.result_queue = result_queue
        self.session = utils.get_session()
        
    def run(self):
        log = logging.getLogger()
        while True:
            doc = self.result_queue.get()
            if doc is None:
                log.info("Nothing left to save for worker %s" % self.name)
                break
            log.info("Saver %s is working on %s" % (self.name, doc['bibcode']))
            try:
                self.session.store_doc(doc)
            except:
                raise
        
def get_bibcodes(opts):
    
    if opts.infile:
        if opts.infile == '-':
            stream = sys.stdin
        else:
            stream = open(opts.infile, 'r')
        bibcodes = itertools.imap(lambda x: x.strip(), stream)
    elif opts.source_model:
        try:
            source_model = eval('models.' + opts.source_model)
            assert hasattr(source_model, 'class_name')
        except AssertionError, e:
            raise Exception("Invalid source_model value: %s" % e)
        session = utils.get_session()
        bibcodes = itertools.imap(lambda x: x.bibcode, session.iterate(source_model))
        
    if opts.limit:
        bibcodes = itertools.islice(bibcodes, opts.limit)
    
    return bibcodes
    
def build_synchronous(opts):
    session = utils.get_session()
    for bib in get_bibcodes(opts):
        doc = session.generate_doc(bib)
        if doc is not None:
            saved = session.store_doc(doc)
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
    savers = [ Saver(results) for i in xrange(opts.threads)]
    for s in savers:
        s.start()
        
    # queue up the bibcodes
    for bib in get_bibcodes(opts):
        tasks.put(bib)
    
    # add some poison pills to the end of the queue
    log.info("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)
    
    # join the results queue. this should
    # block until all tasks in the task queue are completed
    log.info("Joining the builder threads")
    tasks.join()
    
    # poison our saver threads
    log.info("poisoning our result threads")
    for i in xrange(opts.threads):
        results.put(None)
    
    log.info("Joining the saver threads")
    for s in savers:
        s.join()
        
    log.info("All work complete")

def status(opts):
    pass

if __name__ == "__main__":
    
    commands = ['build','build_synchronous', 'status']
    
    op = OptionParser()
    op.set_usage("usage: sync_mongo_data.py [options] [%s]" % '|'.join(commands))
    op.add_option('-i', '--infile', dest="infile", action="store")
    op.add_option('-s', '--source_model', dest="source_model", action="store", default="Accno")
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