'''
Created on Feb 28, 2013

@author: jluker
'''

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
import itertools
from optparse import OptionParser
from multiprocessing import Process, JoinableQueue, cpu_count

from adsdata import utils, models

commands = utils.commandList()

class Builder(Process):
    
    def __init__(self, task_queue, result_queue, do_docs=True, do_metrics=True):
        Process.__init__(self)
        self.do_docs = do_docs
        self.do_metrics = do_metrics
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.session = utils.get_session(config)
        
    def run(self):
        log = logging.getLogger()
        while True:
            bibcode = self.task_queue.get()
            if bibcode is None:
                log.info("Nothing left to build for worker %s", self.name)
                self.task_queue.task_done()
                break
            log.info("Worker %s: working on %s", self.name, bibcode)
            try:
                if self.do_docs:
                    doc = self.session.generate_doc(bibcode)
                    self.session.store(doc, self.session.docs)
                if self.do_metrics:
                    metrics = self.session.generate_metrics_data(bibcode)
                    self.session.store(metrics, self.session.metrics_data)
            except:
                log.error("Something went wrong building %s", bibcode)
                raise
            finally:
                self.task_queue.task_done()
                log.debug("task queue size: %d", self.task_queue.qsize())
        return

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
        session = utils.get_session(config)
        bibcodes = itertools.imap(lambda x: x.bibcode, session.iterate(source_model))
        
    if opts.limit:
        bibcodes = itertools.islice(bibcodes, opts.limit)
    
    return bibcodes
    
@commands
def build_synchronous(opts):
    session = utils.get_session(config)
    for bib in get_bibcodes(opts):
        if 'doc' in opts.do:
            doc = session.generate_doc(bib)
            if doc is not None:
                session.store(doc, session.docs)
        if 'metrics' in opts.do:
            metrics = session.generate_metrics_data(bib)
            if metrics is not None:
                session.store(metrics, session.metrics_data)
        log.info("Done building %s", bib)
    return
        
@commands
def build(opts):
    tasks = JoinableQueue()
    results = JoinableQueue()
    
    if opts.remove:
        log.info("Removing existing docs and metrics_data collection")
        session = utils.get_session(config)
        session.docs.drop()
        session.metrics_data.drop()
        
    do_docs = 'docs' in opts.do
    do_metrics = 'metrics' in opts.do
    
    # start up our builder threads
    log.info("Creating %d Builder processes" % opts.threads)
    builders = [ Builder(tasks, results, do_docs, do_metrics) for i in xrange(opts.threads)]
    for b in builders:
        b.start()
        
    # queue up the bibcodes
    for bib in get_bibcodes(opts):
        tasks.put(bib)
    
    # add some poison pills to the end of the queue
    log.info("poisoning our task threads")
    for i in xrange(opts.threads):
        tasks.put(None)
    
    # join the results queue. this should
    # block until all tasks in the task queue are completed
    log.info("Joining the task queue")
    tasks.join()
    log.info("Joining the task threads")
    for b in builders:
        b.join()
    
    log.info("All work complete")

def status(opts):
    pass

if __name__ == "__main__":
    
    op = OptionParser()
    op.set_usage("usage: build_docs.py [options] [%s]" % '|'.join(commands.map.keys()))
    op.add_option('--do', dest="do", action="append", default=['docs','metrics'])
    op.add_option('-i', '--infile', dest="infile", action="store")
    op.add_option('-s', '--source_model', dest="source_model", action="store", default="Accno")
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=int(cpu_count() / 2))
    op.add_option('-l','--limit', dest="limit", action="store", type=int)
    op.add_option('-r','--remove', dest="remove", action="store_true", default=False)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    op.add_option('--profile', dest='profile', action='store_true',
        help='capture program execution profile', default=False)
    op.add_option('--pygraph', dest='pygraph', action='store_true',
        help='capture exec profile in a call graph image', default=False)
    opts, args = op.parse_args() 
    
    config = utils.load_config()

    log = utils.init_logging(utils.base_dir(), __file__, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)

    try:
        cmd = args.pop()
        assert cmd in commands.map
    except (IndexError,AssertionError):
        op.error("missing or invalid command")
        
    start_cpu = time.clock()
    start_real = time.time()        
    
    if opts.profile:
        import profile
        profile.run("%s(opts)" % cmd, "profile.out")
    else:
        if opts.pygraph:
            import pycallgraph
            pycallgraph.start_trace()

        commands.map[cmd](opts)

        if opts.pygraph: 
            pycallgraph.make_dot_graph('profile.png')
    
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)
