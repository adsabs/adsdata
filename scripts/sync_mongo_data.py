
import os
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
import logging
from datetime import datetime
from optparse import OptionParser
from multiprocessing import Pool, current_process

import mongodb
from config import config

def init_logging(logfile, verbose=False, debug=False):
    logfile = logfile + "." + datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(
        filename = logfile, 
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s'
    )
    log = logging.getLogger()
    log.debug("logging to %s" % logfile)
    if verbose:
        log.addHandler(logging.StreamHandler(sys.stdout))
        log.debug("logging to stdout")
    if debug:
        log.setLevel(logging.DEBUG)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(thread)d %(filename)s %(lineno)d %(message)s')
        for h in log.handlers:
            h.setFormatter(fmt)
        log.debug("debug level logging enabled")
    return log

def load_data(model_class):
    log = logging.getLogger()
    log.debug("thread '%s' working on %s" % (current_process().name, model_class))
    model_class.load_data(batch_size=config.MONGO_DATA_LOAD_BATCH_SIZE)
    
def sync(opts):
    """
    updates the mongo data collections from their data source files
    """
    log = logging.getLogger()
    if opts.debug:
        log.setLevel(logging.DEBUG)
    updates = []
    for model_class in mongodb.data_models():
        if opts.collection and opts.collection != model_class.config_collection_name:
            log.info("skipping %s" % model_class.config_collection_name)
            continue
        if model_class.needs_sync() or opts.force:
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
    for model_class in mongodb.data_models():
        log.info("%s needs sync? : %s" % (model_class.config_collection_name, model_class.needs_sync()))
    
if __name__ == "__main__":
    
    commands = ['sync','status']
    
    op = OptionParser()
    op.set_usage("usage: sync_mongo_data.py [options] [%s]" % '|'.join(commands))
    op.add_option('-c','--collection', dest="collection", action="store", type=str, default=None)
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=4)
    op.add_option('-f','--force', dest="force", action="store_true", default=False)
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

