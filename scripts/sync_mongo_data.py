
import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import logging
from optparse import OptionParser
from multiprocessing import Pool, current_process, cpu_count

from adsdata import models
from adsdata import utils
from adsdata.session import DataSession
from config import config

    
def load_data(model_class):
    log = logging.getLogger()
    log.debug("thread '%s' working on %s" % (current_process().name, model_class))
    session = utils.get_session()
    model_class.load_data(session, batch_size=config.MONGO_DATA_LOAD_BATCH_SIZE)
    
def get_models(opts):
    for model_class in models.data_file_models():
        if len(opts.collection) and model_class.config_collection_name not in opts.collection:
            log.debug("skipping %s" % model_class.config_collection_name)
            continue
        yield model_class
        
def sync(opts):
    """
    updates the mongo data collections from their data source files
    """
    log = logging.getLogger()
    if opts.debug:
        log.setLevel(logging.DEBUG)
        
    session = utils.get_session()
    
    updates = []
    for model_class in get_models(opts):
        if model_class.needs_sync(session) or opts.force:
            updates.append(model_class)
        else:
            log.info("%s does not need syncing" % model_class.config_collection_name)
    if opts.threads > 0:
        p = Pool(opts.threads)
        p.map(load_data, updates)
    else:
        for cls in updates:
            load_data(cls)
        
def status(opts):
    """
    reports on update status of mongo data collections
    """
    log = logging.getLogger()
    if opts.debug:
        log.setLevel(logging.DEBUG)
    session = utils.get_session()
    for model_class in get_models(opts):
        needs_sync = model_class.needs_sync(session) and 'yes' or 'no'
        last_synced = model_class.last_synced(session)
        log.info("%s last synced: %s; needs sync? : %s" % (model_class.config_collection_name, last_synced, needs_sync))
    
if __name__ == "__main__":
    
    commands = ['sync','status']
    
    op = OptionParser()
    op.set_usage("usage: sync_mongo_data.py [options] [%s]" % '|'.join(commands))
    op.add_option('-c','--collection', dest="collection", action="append", default=[])
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=cpu_count())
    op.add_option('-f','--force', dest="force", action="store_true", default=False)
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
