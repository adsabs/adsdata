'''
Created on Sep 18, 2012

@author: jluker
'''
import os
import csv
from config import config
from stat import ST_MTIME
from datetime import datetime

from . import mongodb
import exceptions as exc

from .utils import map_reduce_listify

import logging
log = logging.getLogger(__name__)

    
class DataLoadTime(mongodb.Document):
    
    config_collection_name = 'data_load_time'
    
    collection = mongodb.StringField()
    last_synced = mongodb.DateTimeField()
    
class DataCollection(mongodb.Document):
    
    field_order = []
    aggregated = False
    
    @classmethod
    def last_synced(cls):
        mongo = mongodb.get_mongo()
        collection_name = cls.config_collection_name
        dlt = mongo.query(DataLoadTime).filter(DataLoadTime.collection == collection_name).first()
        if not dlt:
            return None
        log.debug("%s last synced: %s" % (collection_name, dlt.last_synced))
        return dlt.last_synced
    
    @classmethod
    def last_modified(cls):
        collection_name = cls.config_collection_name
        source_file = cls.get_source_file()
        log.info("checking freshness of %s collection vs %s" % (collection_name, source_file))
        modified = datetime.fromtimestamp(os.stat(source_file)[ST_MTIME])
        log.debug("%s last modified: %s" % (source_file, modified))
        return modified
        
    @classmethod
    def needs_sync(cls):
        """
        compare the modification time of a data source
        to its last_synced time in the data_load_time collection
        """
        collection_name = cls.config_collection_name
        last_modified = cls.last_modified()
        last_synced = cls.last_synced()
        
        if not last_synced or last_modified > last_synced:
            log.info("%s needs updating" % collection_name)
            return True
        else:
            log.info("%s does not need updating" % collection_name)
            return False
        
    @classmethod
    def get_source_file(cls):
        collection_name = cls.config_collection_name
        try:
            return config.MONGO_COLLECTIONS[collection_name]
        except:
            raise exc.ConfigurationError("No source file configured for %s" % collection_name)
        
    @classmethod
    def load_data(cls, batch_size=1000, source_file=None, partial=False):
        """
        batch load entries from a data file to the corresponding mongo collection
        """
        mongo = mongodb.get_mongo() 
        
        collection_name = cls.config_collection_name
        if cls.aggregated:
            load_collection_name = collection_name + '_load'
        else:
            load_collection_name = collection_name
        collection = mongo.db[load_collection_name]
        collection.drop()
        log.info("loading data into %s" % load_collection_name)
        
        if not source_file:
            source_file = cls.get_source_file()
        
        def get_collection_field_name(field):
            if field.is_id and cls.aggregated:
                return "load_key"
            else:
                return field.db_field
            
        fields = [get_collection_field_name(x) for x in cls.field_order]
        try:
            fh = open(source_file, 'r')
        except IOError, e:
            log.error(str(e))
            return

        try:
            restkey = cls.restkey
        except:
            restkey = "unwanted"

        reader = csv.DictReader(fh, fields, delimiter="\t", restkey=restkey)
        log.info("inserting records into %s..." % load_collection_name)
        
        batch = []
        batch_num = 1
        while True:
            try:
                record = reader.next()
                if record.has_key('unwanted'):
                    del record['unwanted']
            except StopIteration:
                break
            batch.append(record)
            if len(batch) >= batch_size:
                log.info("inserting batch %d into %s" % (batch_num, load_collection_name))
                collection.insert(batch, safe=True)
                batch = []
                batch_num += 1

        if len(batch):
            log.info("inserting final batch into %s" % load_collection_name)
            collection.insert(batch, safe=True)

        log.info("done loading %d records into %s" % (collection.count(), load_collection_name))

        cls.post_load_data(collection)
        
        dlt = DataLoadTime(collection=collection_name, last_synced=datetime.now())
        mongo.update(dlt, DataLoadTime.collection == collection_name, upsert=True)
        log.info("%s load time updated to %s" % (collection_name, str(dlt.last_synced)))
        
    @classmethod
    def post_load_data(cls, *args, **kwargs):
        """
        this method gets called immediately following the data load.
        subclasses should override to do things like generate
        new collections using map-reduce on the original data
        """
        pass
    
class Bibstem(DataCollection):
    bibstem = mongodb.StringField()
    type_code = mongodb.EnumField(mongodb.StringField(), "R", "J", "C")
    journal_name = mongodb.StringField()
    
    config_collection_name = 'bibstems'
    field_order = [bibstem,type_code,journal_name]
    
    def __str__(self):
        return "%s (%s): %s" % (self.bibstem, self.dunno, self.journal_name)
    
class FulltextLink(DataCollection):
    bibcode = mongodb.StringField(_id=True)
    fulltext_source = mongodb.ListField(mongodb.StringField())
    database = mongodb.SetField(mongodb.StringField())
    provider = mongodb.StringField()
    
    config_collection_name = 'fulltext_links'
    field_order = [bibcode,fulltext_source,database,provider]
    
    def __str__(self):
        return "%s: %s" % (self.bibcode, self.fulltext_source)

class Readers(DataCollection):
    
    bibcode = mongodb.StringField(_id=True)
    readers = mongodb.SetField(mongodb.StringField())
    
    aggregated = True
    config_collection_name = 'readers'
    field_order = [bibcode, readers]
    
    def __str__(self):
        return "%s: [%s]" % (self.bibcode, self.readers)
    
    @classmethod
    def post_load_data(cls, source_collection):
        target_collection_name = cls.config_collection_name
        map_reduce_listify(source_collection, target_collection_name, 'load_key', 'readers')
    
class References(DataCollection):
    
    bibcode = mongodb.StringField(_id=True)
    references = mongodb.SetField(mongodb.StringField())
    
    aggregated = True
    config_collection_name = 'references'
    field_order = [bibcode, references]
    
    def __str__(self):
        return "%s: [%s]" % (self.bibcode, self.references)
    
    @classmethod
    def post_load_data(cls, source_collection):
        target_collection_name = cls.config_collection_name
        map_reduce_listify(source_collection, target_collection_name, 'load_key', 'references')
    
class Refereed(DataCollection):

    bibcode = mongodb.StringField(_id=True)
    
    config_collection_name = 'refereed'
    field_order = [bibcode]
    
    def __str__(self):
        return self.bibcode
    
class DocMetrics(DataCollection):
    bibcode = mongodb.StringField(_id=True)
    boost = mongodb.FloatField()
    citations = mongodb.IntField()
    reads = mongodb.IntField()
    
    config_collection_name = 'docmetrics'
    field_order = [bibcode,boost,citations,reads]
    
    def __str__(self):
        return "%s: %s, %s, %s" % (self.bibcode, self.boost, self.citations, self.reads)
    
class Accno(DataCollection):

    bibcode = mongodb.StringField(_id=True)
    accno = mongodb.StringField()

    config_collection_name = 'accnos'
    field_order = [bibcode,accno]

    def __str__(self):
        return "%s: %s" % (self.bibcode, self.accno)
    
class EprintMatches(DataCollection):

    ecode = mongodb.StringField(_id=True)
    bibcode = mongodb.StringField()

    config_collection_name = 'eprint_matches'
    field_order = [ecode,bibcode]

    def __str__(self):
        return "%s: %s" % (self.ecode, self.bibcode)

class EprintMapping(DataCollection):

    arxivid = mongodb.StringField(_id=True)
    bibcode = mongodb.StringField()

    config_collection_name = 'eprint_mapping'
    field_order = [bibcode,arxivid]

    def __str__(self):
        return "%s: %s" % (self.arxivid, self.bibcode)

class ADSReadsNumbers(DataCollection):

    bibcode = mongodb.StringField(_id=True)
    reads   = mongodb.ListField(mongodb.StringField())

    restkey = 'reads'

    config_collection_name = 'ads_reads_numbers'
    field_order = [bibcode]

    def __str__(self):
        return self.bibcode
        
class ADSDownloadsNumbers(DataCollection):

    bibcode = mongodb.StringField(_id=True)
    downloads = mongodb.ListField(mongodb.StringField())

    restkey = 'downloads'

    config_collection_name = 'ads_downloads_numbers'
    field_order = [bibcode]

    def __str__(self):
        return self.bibcode
