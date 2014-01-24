'''
Created on Sep 18, 2012

@author: jluker
'''
import os
import sys
import csv
import pytz
import inspect
from bson import DBRef
from stat import ST_MTIME
from datetime import datetime
import pymongo
from mongoalchemy import fields
from mongoalchemy.document import Document

from adsdata import utils

import logging
log = logging.getLogger(__name__)
    
def _get_models(cls):
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and cls in obj.__bases__:
            yield obj
    
def data_file_models():
    return _get_models(DataFileCollection)

def doc_source_models():
    return _get_models(DocsDataCollection)

def metrics_data_source_models():
    return _get_models(MetricsDataCollection)

class DataLoadTime(Document):
    
    config_collection_name = 'data_load_time'
    
    collection = fields.StringField()
    last_synced = fields.DateTimeField()
    
class DataCollection(Document):
    """
    This super class exists only to make it easy to collect and operate
    on all the various models via one base class
    """
    pass

class DocsDataCollection(DataCollection):
    
    docs_fields = []
    docs_ref_fields = []
    
    @classmethod
    def get_entry(cls, session, bibcode):
        collection = session.get_collection(cls.config_collection_name)
        return collection.find_one({'_id': bibcode})

    @classmethod
    def add_docs_data(cls, doc, session, bibcode):
        entry = cls.get_entry(session, bibcode)
        if entry:
            for field in cls.docs_fields:
                key = field.db_field
                doc[key] = entry.get(key)
            for ref_field in cls.docs_ref_fields:
                key = ref_field.db_field
                doc[key] = DBRef(collection=cls.config_collection_name, id=bibcode)
                
class MetricsDataCollection(DataCollection):

    docs_fields = []
    docs_ref_fields = []

    @classmethod
    def get_entry(cls, session, bibcode):
        collection = session.get_collection(cls.config_collection_name)
        return collection.find_one({'_id': bibcode})

    @classmethod
    def add_metrics_data(cls, doc, session, bibcode):
        entry = cls.get_entry(session, bibcode)
        if entry:
            for field in cls.docs_fields:
                key = field.db_field
                doc[key] = entry.get(key)

class Fulltext(DocsDataCollection):
    
    bibcode = fields.StringField(_id=True)
    full = fields.StringField()
    ack = fields.StringField(default=None)
    
    config_collection_name = "fulltext"
    docs_fields = [full,ack]
    
    def __str__(self):
        return "Fulltext(%s)" % self.bibcode
    
class DataFileCollection(DataCollection):
    
    field_order = []
    aggregated = False
    restkey = "unwanted"
    
    @classmethod
    def last_synced(cls, session):
        collection_name = cls.config_collection_name
        dlt = session.query(DataLoadTime).filter(DataLoadTime.collection == collection_name).first()
        if not dlt:
            return None
        log.debug("%s last synced: %s" % (collection_name, dlt.last_synced))
        return dlt.last_synced
    
    @classmethod
    def last_modified(cls, data_file):
        collection_name = cls.config_collection_name
        log.debug("checking freshness of %s collection vs %s" % (collection_name, data_file))
        modified = datetime.fromtimestamp(os.stat(data_file)[ST_MTIME]).replace(tzinfo=pytz.utc)
        log.debug("%s last modified: %s" % (data_file, modified))
        return modified
        
    @classmethod
    def needs_sync(cls, session, data_file):
        """
        compare the modification time of a data source
        to its last_synced time in the data_load_time collection
        """
        collection_name = cls.config_collection_name
        last_modified = cls.last_modified(data_file)
        last_synced = cls.last_synced(session)
        
        if not last_synced or last_modified > last_synced:
            log.debug("%s needs updating" % collection_name)
            return True
        else:
            log.debug("%s does not need updating" % collection_name)
            return False
        
    @classmethod
    def load_data(cls, session, data_file, batch_size=1000, partial=False):
        """
        batch load entries from a data file to the corresponding mongo collection
        """
        
        collection_name = cls.config_collection_name
        load_collection_name = collection_name + '_load'
        collection = session.get_collection(load_collection_name)

        # calculate the last_synced timestamp now so that any changes to the source
        # during loading will still trigger a new sync
        dlt = DataLoadTime(collection=collection_name, last_synced=datetime.utcnow().replace(tzinfo=pytz.utc))

        log.debug("loading data into %s" % load_collection_name)
        
        def get_collection_field_name(field):
            if field.is_id and cls.aggregated:
                return "load_key"
            else:
                return field.db_field
            
        try:
            fh = open(data_file, 'r')
        except IOError, e:
            log.error(str(e))
            return

        field_names = [get_collection_field_name(x) for x in cls.field_order]
        reader = csv.DictReader(fh, field_names, delimiter="\t", restkey=cls.restkey, restval="")
        
        cls.insert_records(reader, collection, batch_size)
        
        log.debug("done loading %d records into %s" % (collection.count(), load_collection_name))

        cls.post_load_data(session, collection)
        
        session.update(dlt, DataLoadTime.collection == collection_name, upsert=True)
        log.debug("%s load time updated to %s" % (collection_name, str(dlt.last_synced)))
        
    @classmethod
    def insert_records(cls, reader, collection, batch_size):
        log.debug("inserting records into %s..." % collection.name)
        
        batch = []
        batch_num = 1
        
        def insert_batch(batch):
            try:
                collection.insert(batch, continue_on_error=True)
            except pymongo.errors.DuplicateKeyError, e:
                log.error(e)
            
        while True:
            try:
                record = reader.next()
                if record.has_key('unwanted'):
                    del record['unwanted']
            except StopIteration:
                break
            cls.coerce_types(record)
            batch.append(record)
            if len(batch) >= batch_size:
                log.debug("inserting batch %d into %s", batch_num, collection.name)
                insert_batch(batch)
                batch = []
                batch_num += 1

        if len(batch):
            log.debug("inserting final batch into %s" % collection.name)
            insert_batch(batch)

    @classmethod
    def coerce_types(cls, record):
        """
        given a dict produced by the csv DictReader, will transorm the
        any string values to int or float according to the types defined in the model
        """
        convert_types = [int, float]
        
        def get_constructor(field):
            if hasattr(field, 'constructor'):
                return field.constructor
            else:
                if hasattr(field, 'child_type'):
                    item_type = model_field.child_type()
                elif hasattr(field, 'item_type'):
                    item_type = field.item_type
                return item_type.constructor
            return None
                
        for k, v in record.iteritems():
            # assume id's are strings and we don't need to process
            # (_id field is called "load_key" for aggregated collections
            if k in ['_id','load_key']: 
                continue
            if not v: 
                continue
            model_field = cls.get_fields()[k]
            constructor = get_constructor(model_field)
            if constructor and constructor in convert_types:
                if type(v) is list:
                    record[k] = [constructor(x) for x in v]
                else:
                    record[k] = constructor(v)
        
    @classmethod
    def post_load_data(cls, session, source_collection, *args, **kwargs):
        """
        This method gets called immediately following the data load.
        For normal collections it temporarily saves the existing collection
        to "foo_prev", renames the "foo_load" collection to
        the actual collection name, then cleans up the "foo_prev" collection if
        everything went OK.
        Subclasses should override to do things like generate
        new collections using map-reduce on the original data, e.g., aggregated
        collections do this in a different way.
        """
        cls.swap_in_load_collection(session, source_collection)
        
    @classmethod
    def swap_in_load_collection(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        target_collection = session.get_collection(target_collection_name)
        
        prev_collection_name = target_collection_name + '_prev'
        prev_collection = session.get_collection(prev_collection_name)

        prev_collection.drop()

        if target_collection.count() > 0:
            try:
                target_collection.rename(prev_collection_name)
                log.debug("%s collection renamed to %s", target_collection_name, prev_collection_name)

            except pymongo.errors.OperationFailure, e:
                log.error("unable to rename existing %s collection: %s", target_collection_name, e)
                raise
        
        # rename the newly built collection
        try:
            source_collection.rename(target_collection_name)
            log.debug("new collection renamed to %s", target_collection_name)
            
        except pymongo.errors.OperationFailure, e:
            log.error("unable to rename new collection: %s", e)
            if prev_collection.count() > 0:
                log.debug("restoring from previously saved collection")
                try:
                    prev_collection.rename(target_collection_name)
                except pymongo.errors.OperationFailure, e:
                    log.error("well, crap, something's gone seriously wrong: %s", e)
                    raise
        
        prev_collection.drop()
    
class Bibstem(DataFileCollection):
    bibstem = fields.StringField()
    type_code = fields.EnumField(fields.StringField(), "R", "J", "C")
    journal_name = fields.StringField()
    
    config_collection_name = 'bibstems'
    field_order = [bibstem,type_code,journal_name]
    
    def __str__(self):
        return "Bibstem(%s): %s (%s)" % (self.bibstem, self.journal_name, self.type_code)

class BibstemRanked(DataFileCollection):
    weight = fields.IntField(default=1)
    value = fields.StringField()
    label = fields.StringField()
    
    config_collection_name = 'bibstems_ranked'
    field_order = [weight,value,label]
    
    def __str__(self):
        return "BibstemRanked(%s): %s (%s)" % (self.value, self.label, self.weight)
    
    @classmethod
    def post_load_data(cls, session, source_collection, *args, **kwargs):
        """
        conctatenate the label and value fields like so that 
        label = "label (value)"
        """
        log.debug("munging ranked bibstem labels")
        for d in source_collection.find(snapshot=True):
            d['label'] = "%s (%s)" % (d.get('label'), d.get('value'))
            source_collection.save(d)
        super(BibstemRanked, cls).post_load_data(session, source_collection, *args, **kwargs) 
    
class FulltextLink(DataFileCollection):
    bibcode = fields.StringField(_id=True)
    fulltext_source = fields.StringField()
    database = fields.StringField(default="")
    provider = fields.StringField(default="")
    
    config_collection_name = 'fulltext_links'
    field_order = [bibcode,fulltext_source,database,provider]
    
    def __str__(self):
        return "FulltextLink(%s): %s" % (self.bibcode, self.fulltext_source)

class Readers(DataFileCollection, DocsDataCollection):
    
    bibcode = fields.StringField(_id=True)
    readers = fields.ListField(fields.StringField())
    
    aggregated = True
    config_collection_name = 'readers'
    field_order = [bibcode, readers]
    docs_fields = [readers]
    
    def __str__(self):
        return "Readers(%s): [%s]" % (self.bibcode, self.readers)
    
    @classmethod
    def post_load_data(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        utils.map_reduce_listify(session, source_collection, target_collection_name, 'load_key', 'readers')
    
class References(DataFileCollection):
    
    bibcode = fields.StringField(_id=True)
    references = fields.ListField(fields.StringField())
    
    aggregated = True
    config_collection_name = 'references'
    field_order = [bibcode, references]
    
    def __str__(self):
        return "References(%s): [%s]" % (self.bibcode, self.references)
    
    @classmethod
    def post_load_data(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        utils.map_reduce_listify(session, source_collection, target_collection_name, 'load_key', 'references')

class Citations(DataFileCollection, DocsDataCollection, MetricsDataCollection):
    
    bibcode = fields.StringField(_id=True)
    citations = fields.ListField(fields.StringField())
    
    aggregated = True
    config_collection_name = 'citations'
    field_order = [bibcode, citations]
    docs_fields = [citations]
    
    def __str__(self):
        return "Citations(%s): [%s]" % (self.bibcode, self.citations)
    
    @classmethod
    def add_metrics_data(cls, doc, session, bibcode):
        today = datetime.today()
        age = max(1.0, today.year - int(bibcode[:4]) + 1)
        entry = cls.get_entry(session, bibcode)
        try:
            citations = entry.get('citations',[])
        except:
            citations = []
        refereed_collection = session.get_collection('refereed')
        refereed = False
        if refereed_collection.find_one({'_id':bibcode}):
            refereed = True
        reference_collection = session.get_collection('references')
        ref_norm = 0.0
        for citation in citations:
            try:
                res = reference_collection.find_one({'_id':citation})
                Nrefs = len(res.get('references',[]))
                ref_norm += 1.0/float(max(5, Nrefs))
            except:
                pass
        doc['refereed'] = refereed
        doc['citations'] = citations
        doc['citation_num'] = len(doc['citations'])
        doc['refereed_citations'] = filter(lambda a: refereed_collection.find_one({'_id':a}), doc['citations'])
        doc['refereed_citation_num'] = len(doc['refereed_citations'])
        doc['an_citations'] = float(doc['citation_num'])/float(age)
        doc['an_refereed_citations'] = float(doc['refereed_citation_num'])/float(age)
        doc['rn_citations'] = ref_norm

    @classmethod
    def post_load_data(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        utils.map_reduce_listify(session, source_collection, target_collection_name, 'load_key', 'citations')
    
class Refereed(DataFileCollection, DocsDataCollection):

    bibcode = fields.StringField(_id=True)
    
    config_collection_name = 'refereed'
    field_order = [bibcode]
    docs_fields = [bibcode]
    
    @classmethod
    def add_docs_data(cls, doc, session, bibcode):
        entry = cls.get_entry(session, bibcode)
        if entry:
            doc['refereed'] = True
                
    def __str__(self):
        return "Refereed(%s)" % self.bibcode
    
class DocMetrics(DataFileCollection, DocsDataCollection):
    bibcode = fields.StringField(_id=True)
    boost = fields.FloatField(default=0.0)
    citation_count = fields.IntField(default=0)
    read_count = fields.IntField(default=0)
    norm_cites = fields.IntField(default=0)
    
    config_collection_name = 'docmetrics'
    field_order = [bibcode,boost,citation_count,read_count,norm_cites]
    docs_fields = [boost, citation_count, read_count, norm_cites]
    
    def __str__(self):
        return "DocMetrics(%s): %s, %s, %s" % (self.bibcode, self.boost, self.citations, self.reads)

# add Simbad Object ids
class SimbadObjectIDs(DataFileCollection, DocsDataCollection):
    
    bibcode = fields.StringField(_id=True)
    simbad_object_ids = fields.ListField(fields.IntField())
    
    aggregated = True
    config_collection_name = 'simbad_object_ids'
    field_order = [bibcode, simbad_object_ids]
    docs_fields = [simbad_object_ids]
    
    def __str__(self):
        return "Simbad_objs(%s): [%s]" % (self.bibcode, self.simbad_object_ids)
    
    @classmethod
    def post_load_data(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        utils.map_reduce_listify(session, source_collection, target_collection_name, 'load_key', 'simbad_object_ids')
    

class Accno(DataFileCollection):

    bibcode = fields.StringField(_id=True)
    accno = fields.StringField()

    config_collection_name = 'accnos'
    field_order = [bibcode,accno]

    def __str__(self):
        return "Accno(%s): %s" % (self.bibcode, self.accno)
    
class EprintMatches(DataFileCollection):

    ecode = fields.StringField(_id=True)
    bibcode = fields.StringField()

    config_collection_name = 'eprint_matches'
    field_order = [ecode,bibcode]

    def __str__(self):
        return "EprintMatches(%s): %s" % (self.ecode, self.bibcode)

class EprintMapping(DataFileCollection):

    arxivid = fields.StringField(_id=True)
    bibcode = fields.StringField()

    config_collection_name = 'eprint_mapping'
    field_order = [bibcode,arxivid]

    def __str__(self):
        return "EprintMapping(%s): %s" % (self.arxivid, self.bibcode)

class Reads(DataFileCollection, DocsDataCollection, MetricsDataCollection):

    bibcode = fields.StringField(_id=True)
    reads   = fields.ListField(fields.IntField())

    restkey = 'reads'

    config_collection_name = 'reads'
    field_order = [bibcode]
    docs_fields = [reads]

    def __str__(self):
        return "Reads(%s)" % self.bibcode
        
class Downloads(DataFileCollection, DocsDataCollection, MetricsDataCollection):

    bibcode = fields.StringField(_id=True)
    downloads = fields.ListField(fields.IntField())

    restkey = 'downloads'

    config_collection_name = 'downloads'
    field_order = [bibcode]
    docs_fields = [downloads]

    def __str__(self):
        return "Downloads(%s)" % self.bibcode
    
class Grants(DataFileCollection, DocsDataCollection):
    
    bibcode = fields.StringField(_id=True)
    agency = fields.StringField()
    grant = fields.StringField()
    grants = fields.ListField(fields.DictField(fields.StringField()))
    
    aggregated = True
    config_collection_name = "grants"
    field_order = [bibcode, agency, grant]
    docs_fields = [grants]
    
    @classmethod
    def post_load_data(cls, session, source_collection):
        target_collection_name = cls.config_collection_name
        utils.map_reduce_dictify(session, source_collection, target_collection_name, 'load_key', ['agency','grant'], 'grants')
    
    def __str__(self):
        return "Grants(%s): %s, %s" % (self.bibcode, self.agency, self.grant)

class Authors(DataFileCollection, MetricsDataCollection):

    bibcode = fields.StringField(_id=True)
    authors = fields.ListField(fields.StringField())

    restkey = 'authors'

    config_collection_name = 'authors'
    field_order = [bibcode]
    docs_fields = [authors]

    @classmethod
    def add_metrics_data(cls, doc, session, bibcode):
        entry = cls.get_entry(session, bibcode)
        try:
            authors = entry.get('authors',[])
        except:
            authors = []
        doc['author_num'] = len(authors)

    def __str__(self):
        return "Authors(%s)" % self.bibcode
