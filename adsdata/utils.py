'''
Created on Oct 11, 2012

@author: jluker

NOTE: it is best to keep the imports in this module limited to what's available
in the python standard library as it contains functions used by both the regular
adsdata modules/scripts and the jython pdf extraction code. Special stuff, like
redis, puka, etc., can be imported at the function scope.
'''

import os
import re
import sys
import json
import string
import unicodedata
from datetime import datetime
import ConfigParser
from stat import ST_MTIME

import logging
log = logging.getLogger()
redis_log = None
        
def init_logging(base_dir, calling_script, logfile=None, verbose=False, debug=False):

    if logfile is None:
        log_dir = os.path.exists(base_dir + "/logs") and base_dir + "/logs" or "."
        logfile = "%s/%s" % (log_dir, os.path.basename(calling_script)) + "." + datetime.now().strftime("%Y%m%d-%H%M%S")

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

def base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

config = None
def load_config(config_file=None):
    """
    NOTE: since this returns a dict of config values, it's conceivable that code
    could change these config values. DO NOT DO THIS!
    """
    global config
    if config is None:
        if config_file is None:
            config_file = os.path.join(base_dir(), 'adsdata.cfg')
            
        config = ConfigParser.ConfigParser()
        config.optionxform = str # otherwise the config parser lowercases setting names
        config.read(config_file)
        items = []
        for k, v in config.items('adsdata'):
            if isinstance(v, str) and v.isdigit():
                items.append((k,int(v)))
            else:
                items.append((k,v))
        items.append(('collections', dict(config.items('collections'))))
        config = dict(items)
    return config

def commandList():
    """
    decorator that allows scripts to register functions to be used as script commands
    """
    registry = {}
    def registrar(func):
        registry[func.__name__] = func
        return func
    registrar.map = registry
    return registrar

def mongo_uri(host, port, db=None, user=None, passwd=None):
    if user and passwd:
        uri = "mongodb://%s:%s@%s:%d/%s" % (user, passwd, host, port, db)
    else:
        uri = "mongodb://%s:%d" % (host, port)
    return uri

def get_session(config, **kwargs):
    from session import DataSession
    uri = mongo_uri(config['ADSDATA_MONGO_HOST'], config['ADSDATA_MONGO_PORT'], 
                    db=config['ADSDATA_MONGO_DATABASE'], 
                    user=config['ADSDATA_MONGO_USER'], 
                    passwd=config['ADSDATA_MONGO_PASSWORD'])
    return DataSession(config['ADSDATA_MONGO_DATABASE'], uri) 

def get_document(session, model, **kwargs):
    doc = session.query(model).filter(kwargs).one()
    if doc is None:
        log.debug("query did not match any entries in %s", model.config_collection_name)
    return doc 

TranslationMap = None
def get_translation_map():
    """ prepare translation map to remove all control characters except
        tab, new-line and carriage-return """
    global TranslationMap
    if not TranslationMap:
        ctrls = trans = ''
        for n in range(0, 32):
            char = chr(n)
            ctrls += char
            if char in '\t\n\r':
                trans += char
            else:
                trans += u' '
        TranslationMap = string.maketrans(ctrls, trans)
    return TranslationMap

UnicodeTranslationMap = None
def get_unicode_translation_map():
    """ prepare translation map to remove all control characters except
        tab, new-line and carriage-return """
    global UnicodeTranslationMap
    if not UnicodeTranslationMap:
        illegal_unichrs = [ (0x00, 0x08), (0x0B, 0x1F), (0x7F, 0x84), (0x86, 0x9F),
            (0xD800, 0xDFFF), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF),
            (0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF),
            (0x4FFFE, 0x4FFFF), (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
            (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF), (0x9FFFE, 0x9FFFF),
            (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
            (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF),
            (0x10FFFE, 0x10FFFF) ]
        UnicodeTranslationMap = dict.fromkeys(r for start, end in illegal_unichrs for r in range(start, end+1))
    return UnicodeTranslationMap

IllegalCharRegex = None

def text_cleanup(text, translate=False, decode=False):
    if translate:
        if type(text) == str:
            tmap = get_translation_map()
        else:
            tmap = get_unicode_translation_map()
        text = text.translate(tmap)
    if decode and type(text) == str:
        text = text.decode('utf-8', 'ignore')
    text = unicodedata.normalize('NFKC', unicode(text))
    text = re.sub('\s+', ' ', text)
    return text            
    
def map_reduce_listify(session, source, target_collection_name, group_key, value_field):
    """
    Will take a 1:many key:value collection and aggregate values in a list by unique key
    """
    from bson.code import Code

    map_func = Code("function(){ " \
                + "emit( this.%s, { '%s': [this.%s] } ); " % (group_key, value_field, value_field) \
            + "};")

    reduce_func = Code("function( key , values ){ " \
                + "var ret = { '%s': [] }; " % value_field \
                + "for ( var i = 0; i < values.length; i++ ) " \
                    + "ret['%s'].push.apply(ret['%s'],values[i]['%s']); " % (value_field, value_field, value_field) \
                + " return ret;" \
            + "};")

    log.info("running map-reduce on %s" % source.name)
    source.map_reduce(map_func, reduce_func, target_collection_name)
    
    target = session.get_collection(target_collection_name)
    log.info("cleaning up target collection: %s" % target_collection_name)
    target.update({}, {'$rename': {('value.' + value_field) : value_field}}, multi=True)
    target.update({}, {'$unset': { 'value': 1 }}, multi=True)
    source.drop()

def map_reduce_dictify(session, source, target_collection_name, group_key, value_fields, output_key=None):
    """
    Will take a 1:many key:multiple-values collection and aggregate values in a list of dicts
    by unique key
    """
    from jinja2 import Template
    if not output_key:
        output_key = target_collection_name
    
    emit_vals = ','.join(["'%s': this.%s" % (x,x) for x in value_fields])
    map_func = Template("""
        function() {
            emit( this.{{ group_key }}, { '{{ output_key }}': [{ {{ emit_vals }} }] });
        };
    """).render(group_key=group_key, emit_vals=emit_vals, output_key=output_key)
    
    reduce_func = Template("""
        function(key, values) {
            var ret = { '{{ output_key }}': [] };
            for ( var i = 0; i < values.length; i++ ) {
                ret['{{ output_key }}'].push.apply(ret['{{ output_key }}'], values[i]['{{ output_key }}']);
            }
            return ret;
        };
    """).render(output_key=output_key)
    
    log.info("running map-reduce on %s" % source.name)
    source.map_reduce(map_func, reduce_func, target_collection_name)
    
    target = session.get_collection(target_collection_name)
    log.info("cleaning up target collection: %s" % target_collection_name)
    target.update({}, {'$rename': {('value.' + output_key) : output_key}}, multi=True)
    target.update({}, {'$unset': { 'value': 1 }}, multi=True)
    source.drop()

def mod_time(file):
    """stat a file to get last mod time
    """
    mtime = os.stat(file)[ST_MTIME]
    return datetime.fromtimestamp(mtime)

def rabbitmq_channel(exchange=None, durable=False):
    import pika
    config = load_config()
    conn = pika.BlockingConnection(pika.URLParameters(config['RABBITMQ_URI']))
    channel = conn.channel()
    return channel

def publish_updates(updates):
    """
    send the list of fulltext records that were updated to rabbitmq
    """
    channel = rabbitmq_channel()
    channel.queue_declare(queue="fulltext_updates")
    msg = json.dumps({ 'updates': updates })
    channel.basic_publish(exchange="", routing_key="fulltext_updates", body=msg)
    