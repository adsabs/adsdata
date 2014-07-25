#!/usr/bin/env python

import os
import re
import sys
import time
import logging
import itertools
import traceback
from datetime import datetime
from optparse import OptionParser
from multiprocessing import Process, JoinableQueue, Lock, Manager

from adsdata import utils, models
from adsdata.extractors import Extractor

config = utils.load_config()
commands = utils.commandList()
log = logging.getLogger()

class ExtractWorker(Process):
    
    def __init__(self, queue, opts, thread_lock, stats, updates):
        Process.__init__(self)
        self.queue = queue
        self.stats = stats
        self.updates = updates
        self.opts = opts
        self.thread_lock = thread_lock
        
    def run(self):
        
        while True:
            ft_item = self.queue.get()
            if ft_item is None:
                self.queue.task_done()
                break
            ext = None
            try:
                ext = Extractor.factory(*ft_item)
                
                if self.opts.dry_run:
                    ext.dry_run = True
                    
                self.thread_lock.acquire()
                try:
                    if not self.opts.force and not ext.needs_extraction(self.opts.force_older_than):
                        continue # note: the finally clause is still executed
                finally:
                    self.thread_lock.release()
                
                updated = ext.extract(clobber=self.opts.clobber)
                if updated:
                    self.updates.append(ext.bibcode)
                
            except Exception, e:
                bibcode, ft_source, provider = ft_item
                log.error("something went wrong extracting %s: %s", bibcode, traceback.format_exc())
                self.stats['exceptions'] += 1
            finally:
                self.stats['processed'] += 1
                self.queue.task_done()

def get_ft_items(opts):
    
    session = utils.get_session(config)
    ft_iter = None
    
    if opts.infile:
        if opts.infile == '-':
            stream = sys.stdin
        else:
            stream = open(opts.infile, 'r')
        bibcodes = itertools.ifilter(lambda(x): re.match('^\d{4}', x) and True or False, stream)
        bibcodes = itertools.imap(lambda x: x.strip(), bibcodes)
        def fetch_ft_link(bib):
            return utils.get_document(session, models.FulltextLink, bibcode=bib)
        ft_iter = itertools.imap(fetch_ft_link, bibcodes)
    else:
        ft_iter = session.iterate(models.FulltextLink)
        
    if opts.limit:
        ft_iter = itertools.islice(ft_iter, opts.limit)
    
    for ft_link in ft_iter:
        if ft_link is None:
            continue
        yield (ft_link.bibcode, ft_link.fulltext_source, ft_link.provider)

@commands
def extract(opts):
    
    tasks = JoinableQueue()
    manager = Manager()
    stats = manager.dict()
    updates = manager.list()
    stats['processed'] = 0
    stats['exceptions'] = 0
    
    # start up our builder threads
    log.info("Creating %d extractor processes" % opts.threads)
    
    thread_lock = Lock()
    workers = [ ExtractWorker(tasks, opts, thread_lock, stats, updates) for i in xrange(opts.threads)]
    for w in workers:
        w.start()
        
    # queue up the bibcodes
    for item in get_ft_items(opts):
        tasks.put(item)
    
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
    log.info("exceptions: %d" % stats['exceptions'])
    log.info("updated: (%d) %s" % (len(updates), ', '.join(updates)))
    
    log.info("All work complete")
    
    if len(updates): # and not opts.dry_run:
        log.info("Publishing list of updated records")
        utils.publish_updates(list(updates))
    
if __name__ == '__main__':

    op = OptionParser()
    op.set_usage("usage: extract_fulltext.py [options] ")
    op.add_option('-i','--infile', dest='infile', action='store',
        help='generate from urls/paths in this file')
    op.add_option('-v','--verbose', dest='verbose', action='store_true',
        help='write log output to stdout', default=False)
    op.add_option('-d','--debug', dest='debug', action='store_true',
        help='include debugging info in log output', default=False)
    op.add_option('-l','--limit', dest="limit", action="store", type=int)
    op.add_option('-f','--force', dest='force', action='store_true',
        help='ignore modtimes', default=False)
    op.add_option('-c','--clobber', dest='clobber', action='store_true',
        help='ignore empty content (always overwrite)', default=False)
    op.add_option('-n','--dry_run', dest='dry_run', action='store_true',
        help='go through all the motions but don\'t write anything to disk', default=False)
    op.add_option('--force_older_than', dest='force_older_than', action='store',
        help='generate docs w/ last generated prior to date in format %Y-%m-%d %H:%M:%S %Z')
    op.add_option('-t','--threads', dest='threads', action='store', type=int,
        help='number of threads to use for extracting (default=12)', default=13)
    op.add_option('--pygraph', dest='pygraph', action='store_true',
        help='capture exec profile in a call graph image', default=False)
    opts, args = op.parse_args()

    log = utils.init_logging(utils.base_dir(), __file__, None, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)
        
    # set pika logging to >= warnings
    import pika
    pika_log = logging.getLogger('pika')
    pika_log.setLevel(logging.WARNING)
    
    if opts.force_older_than:
        log.info("Forcing generationo of records older than %s" % opts.force_older_than)
        opts.force_older_than = datetime(*(time.strptime(opts.force_older_than, '%Y-%m-%d %H:%M:%S %Z')[:5]))
        
    try:
        cmd = args.pop()
        assert cmd in commands.map
    except (IndexError,AssertionError):
        op.error("missing or invalid command")
        
    start_cpu = time.clock()
    start_real = time.time()

    if opts.pygraph:
        from pycallgraph import PyCallGraph
        from pycallgraph.output import GraphvizOutput
        with PyCallGraph(output=GraphvizOutput()):
            commands.map[cmd](opts)
    else:
        commands.map[cmd](opts)

        
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)


