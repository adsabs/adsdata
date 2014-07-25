
import re
import os
import sys
import time
import json
import ptree
import logging
import itertools
from itertools import imap, islice, ifilter
from optparse import OptionParser
from pymongo import MongoClient
from multiprocessing import Process, JoinableQueue, Manager

from adsdata import utils

config = utils.load_config()
commands = utils.commandList()
log = logging.getLogger()

class Worker(Process):
    def __init__(self, queue, opts, stats):
        Process.__init__(self)
        self.opts = opts
        self.queue = queue
        self.stats = stats
    
    def run(self):
        while True:
            doc = self.queue.get()
            if doc is None:
                log.debug("Nothing left to do for worker %s", self.name)
                self.queue.task_done()
                break

            self.stats['processed'] += 1
            log.info("Worker %s is working on %s", self.name, doc['bibcode'])
            
            extract_dir = config['FULLTEXT_EXTRACT_PATH'] + ptree.id2ptree(doc['bibcode'])
            meta_path = os.path.join(extract_dir, 'meta.json')
            log.debug("meta path: %s", meta_path)
            
            # dry-run testing
#            self.queue.task_done()
#            continue
         
            if not os.path.exists(extract_dir):
                log.debug("no existing extract dir for %s", doc['bibcode'])
                self.stats['missing'] += 1
                self.queue.task_done()
                continue
            
            if os.path.exists(meta_path) and not self.opts.force:
                log.debug("found existing meta file for %s", doc['bibcode'])
                self.queue.task_done()
                continue
            
            meta = {
                'ft_source': doc['ft_source'],
                'provider': doc['ft_provider'],
                'index_date': doc['index_date']
                }
            
            log.debug("writing meta file for %s", doc['bibcode'])
            with open(meta_path,'w') as f:
                json.dump(meta, f)
            
            mtime = time.mktime(doc['_generated'].timetuple())
            log.debug("setting mtime for %s to %s, %s", meta_path, doc['_generated'], mtime)
            os.utime(meta_path, (mtime, mtime))
            
            self.queue.task_done()
            
def get_docs(opts):
    """
    accept a list of bibcodes via file or stdin
    or iterate through the docs collection
    """
    
    mongo = MongoClient(opts.mongo_uri)
    db = mongo[opts.mongo_db]
    wanted = {'bibcode': 1, '_generated': 1, 'ft_source': 1, 'ft_provider': 1, 'index_date': 1}
    doc_iter = None
    
    if opts.infile:
        if opts.infile == '-':
            stream = sys.stdin
        else:
            stream = open(opts.infile, 'r')
        bibcodes = itertools.ifilter(lambda(x): re.match('^\d{4}', x) and True or False, stream)
        bibcodes = itertools.imap(lambda x: x.strip(), bibcodes)
        def fetch_doc(bib):
            """map incoming bibcodes to mongo records to the the last generated time"""
            return db.docs.find_one({'bibcode': bib}, wanted)
        doc_iter = itertools.imap(fetch_doc, bibcodes)
    else:
        query = eval(opts.query)
        doc_iter = db.docs.find(query, wanted)
        
    if opts.limit:
        doc_iter = itertools.islice(doc_iter, opts.limit)
    
    for doc in doc_iter:
        if doc is None:
            continue
        yield doc
        
@commands
def init(opts):

    tasks = JoinableQueue()
    manager = Manager()
    stats = manager.dict()
    stats['processed'] = 0
    stats['missing'] = 0
    
    # start up our workers threads
    log.info("Creating %d workers" % opts.threads)
    
    workers = [ Worker(tasks, opts, stats) for i in xrange(opts.threads)]
    for w in workers:
        w.start()
        
    # queue up the bibcodes
    for doc in get_docs(opts):
        tasks.put(doc)
    
    # add some poison pills to the end of the queue
    log.info("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)
    
    # join the results queue. this should
    # block until all tasks in the task queue are completed
    log.info("Joining the task queue")
    tasks.join()
    
    log.info("Joining the task threads")
    for w in workers:
        w.join()
        
    log.info("processed: %d" % stats['processed'])
    log.info("records with no existing extract dir: %d" % stats['missing'])        
    
if __name__ == '__main__':

    op = OptionParser()
    op.set_usage("usage: index.py [options] ")
    op.add_option('-v','--verbose', dest='verbose', action='store_true',
        help='write log output to stdout', default=False)
    op.add_option('-d','--debug', dest='debug', action='store_true',
        help='include debugging info in log output', default=False)
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=8)#cpu_count())# * 2)
    op.add_option('-l','--limit', dest='limit', action='store',
        help='process this many', type=int, default=0)
    op.add_option('-q','--query', dest='query', action='store',
        help='process mongo docs that match this query', type=str, default="{}")
    op.add_option('-u','--mongo_uri', dest='mongo_uri', action='store', type=str,
        help='mongo connection uri in the form "mongodb://user:pass@host/db')
    op.add_option('-b','--mongo_db', dest='mongo_db', action='store', type=str,
        help='mongo database name. "docs" collection is assumed')
    op.add_option('-i','--infile', dest='infile', action='store', type=str,
        help='path to input bibcodes file')
    op.add_option('--force', dest='force', action='store_true',
        help='overwrite any existing meta files', default=False)
    
    opts, args = op.parse_args()

    log = utils.init_logging(utils.base_dir(), __file__, None, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)
        
    try:
        cmd = args.pop()
        assert cmd in commands.map
    except (IndexError,AssertionError):
        op.error("missing or invalid command")
        
    start_cpu = time.clock()
    start_real = time.time()

    commands.map[cmd](opts)

    end_cpu = time.clock()
    end_real = time.time()

    log.info("%f Real Seconds", (end_real - start_real))
    log.info("%f CPU Seconds", (end_cpu - start_cpu))


