'''
Created on Feb 28, 2013

@author: jluker
'''

import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
import logging
import itertools
from datetime import datetime
from optparse import OptionParser
from Queue import Empty as QueueEmpty
from multiprocessing import Process, Queue, cpu_count

from adsdata.session import DataSession
from adsdata.utils import init_logging, has_stdin
from adsdata.models import Accno
from adsdata.exceptions import *
from config import config

class Builder(Process):
    
    def __init__(self, task_queue, result_queue):
        Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.session = DataSession(config.MONGO_DATABASE)
        
    def run(self):
        log = logging.getLogger()
        while True:
            bibcode = self.task_queue.get()
            if bibcode is None:
                log.info("Nothing left to do for worker %s" % self.name)
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
        return

def get_bibcodes(opts):
    session = DataSession(config.MONGO_DATABASE)
    
    if has_stdin():
        bibcodes = itertools.imap(lambda x: x.strip(), sys.stdin)
    else:
        bibcodes = itertools.imap(lambda x: x.bibcode, session.iterate(Accno))
        
    if opts.limit:
        bibcodes = itertools.islice(bibcodes, opts.limit)
    
    return bibcodes
    
def build_synchronous(opts):
    session = DataSession(config.MONGO_DATABASE)
    for bib in get_bibcodes(opts):
        doc = session.generate_doc(bib)
        log.info("Result: %s" % str(doc))
    return
        
def build(opts):
    tasks = Queue()
    results = Queue()
    
    # start up our builder threads
    log.info("Creating %d Builder processes" % opts.threads)
    builders = [ Builder(tasks, results) for i in xrange(opts.threads)]
    for b in builders:
        b.start()
        
    # queue up the bibcodes
    for bib in get_bibcodes(opts):
        tasks.put(bib)
    
    # add some poison pills to the end of the queue
    for i in xrange(opts.threads):
        tasks.put(None)
        
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
    op.add_option('-c','--collection', dest="collection", action="store", type=str, default=None)
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=cpu_count() * 2)
    op.add_option('-l','--limit', dest="limit", action="store", type=int)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    logfile = "%s/%s" % (config.LOG_DIR, os.path.basename(__file__))
    log = init_logging(logfile, opts.verbose, opts.debug)
    
    try:
        cmd = args.pop()
        assert cmd in commands
    except (IndexError,AssertionError):
        op.error("missing or invalid command")
        
    eval(cmd)(opts)