
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

commands = utils.commandList()
    
def load_data(model_class, data_file, batch_size):
    log = logging.getLogger()
    log.debug("thread '%s' working on %s" % (current_process().name, model_class))
    session = utils.get_session(config)
    model_class.load_data(session, data_file, batch_size=batch_size)
    
def get_models(opts, config):
    for model_class in models.data_file_models():
        if len(opts.collection) and model_class.config_collection_name not in opts.collection:
            log.debug("skipping %s" % model_class.config_collection_name)
            continue
        collection_name = model_class.config_collection_name
        if collection_name not in config['collections']:
            raise RuntimeError("No source file configured for %s" % collection_name)
        yield model_class, config['collections'][collection_name]
        
@commands
def sync(opts, config):
    """
    updates the mongo data collections from their data source files
    """
    log = logging.getLogger()
        
    session = utils.get_session(config)
    
    updates = []
    for model_class, data_file in get_models(opts, config):
        if model_class.needs_sync(session, data_file) or opts.force:
            log.info("%s needs synching" % model_class.config_collection_name)
            updates.append((model_class, data_file, config['ADSDATA_MONGO_DATA_LOAD_BATCH_SIZE']))
        else:
            log.info("%s does not need syncing" % model_class.config_collection_name)
    if opts.threads > 0:
        p = Pool(opts.threads)
        p.map(load_data, updates)
    else:
        for cls, data_file, batch_size in updates:
            load_data(cls, data_file, batch_size)
        
@commands
def status(opts, config):
    """
    reports on update status of mongo data collections
    """
    log = logging.getLogger()
    session = utils.get_session(config)
    for model_class, data_file in get_models(opts, config):
        needs_sync = model_class.needs_sync(session, data_file) and 'yes' or 'no'
        last_synced = model_class.last_synced(session)
        log.info("%s last synced: %s; needs sync? : %s" % (model_class.config_collection_name, last_synced, needs_sync))
    
if __name__ == "__main__":
    
    op = OptionParser()
    op.set_usage("usage: sync_mongo_data.py [options] [%s]" % '|'.join(commands.map.keys()))
    op.add_option('-c','--collection', dest="collection", action="append", default=[])
    op.add_option('-t','--threads', dest="threads", action="store", type=int, default=cpu_count())
    op.add_option('-f','--force', dest="force", action="store_true", default=False)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-v','--verbose', dest="verbose", action="store_true", default=False)
    opts, args = op.parse_args() 
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config = utils.load_config(os.path.join(base_dir, 'adsdata.cfg'))

    log = utils.init_logging(base_dir, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)
    
    try:
        cmd = args.pop()
        assert cmd in commands.map
    except (IndexError,AssertionError):
        op.error("missing or invalid command")
        
    start_cpu = time.clock()
    start_real = time.time()        
    
    commands.map[cmd](opts, config)
    
    end_cpu = time.clock()
    end_real = time.time()
    
    print "Real Seconds: %f" % (end_real - start_real)
    print "CPU Seconds: %f" % (end_cpu - start_cpu)
