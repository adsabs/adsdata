'''
Created on Jul 10, 2014

@author: jluker
'''


import os
import re
import uuid
import json
import errno
import ptree
import logging
import httplib2
from datetime import datetime
from tempfile import NamedTemporaryFile
from multiprocessing import current_process
from lxml.html.soupparser import fromstring as soupparser
from lxml.html import fromstring as htmlparser
from lxml import etree
from StringIO import StringIO
from os.path import basename

from adsdata import utils
from entitydefs import convertentities

config = utils.load_config()
log = logging.getLogger()

class UnknownSourceTypeException(Exception):
    pass

class FulltextContentsNotFound(Exception):
    pass

class FulltextSourceNotFound(Exception):
    pass

class PdfExtractException(Exception):
    pass

class Extractor():
    
    def __init__(self, bibcode, ft_source, provider):
        
        self.bibcode = bibcode
        self.ft_source = ft_source
        self.provider = provider
        self.extract_dir = config['FULLTEXT_EXTRACT_PATH'] + ptree.id2ptree(bibcode)
        self.meta_path = os.path.join(self.extract_dir, 'meta.json')
        self.source_loaded = False
        self.source_content = None
        self.dry_run = False
        
        self.last_extracted = self.get_last_extracted()
        log.debug("%s last extracted: %s", self.bibcode, self.last_extracted)

    @classmethod
    def factory(cls, bibcode, ft_source, provider):

        if ft_source.lower().startswith('http'):
            log.debug("%s is a http fulltext record", bibcode)
            return HttpExtractor(bibcode, ft_source, provider)

        if ft_source.lower().endswith('pdf'):
            log.debug("%s is a pdf fulltext record", bibcode)
            return PdfExtractor(bibcode, ft_source, provider)

        if ft_source.lower().endswith('xml'):
            log.debug("%s is a xml fulltext record", bibcode)
            
            if provider == 'Elsevier':
                return ElsevierExtractor(bibcode, ft_source, provider)
            else:
                return XMLExtractor(bibcode, ft_source, provider)

        if ft_source.lower().endswith('html'):
            log.debug("%s is a html fulltext record" % bibcode)
            return HtmlExtractor(bibcode, ft_source, provider)

        if ft_source.lower().endswith('txt') or ft_source.lower().endswith('ocr'):
            log.debug("%s is an ocr or plain text fulltext record" % bibcode)
            return PlainTextExtractor(bibcode, ft_source, provider)

        raise UnknownSourceTypeException(
            "don't know what generator class to use for %s, %s, %s", bibcode, ft_source, provider)

    def content_path(self, field):
        return os.path.join(self.extract_dir, "%s.txt" % field)
    
    def init_path(self):
        meta = { 'ft_source': self.ft_source, 'provider': self.provider }
        if not self.dry_run:
            try:
                os.makedirs(self.extract_dir)
                self.write_meta(meta)
            except IOError, e:
                if e.errno == errno.EEXIST and os.path.isdir(self.extract_dir):
                    # another process beat us. no big deal.
                    pass
                else:
                    log.debug("Failed initializing target extraction dir: %s", str(e))
                    raise
        return meta
            
    def get_meta(self):
        with open(self.meta_path, 'r') as f:
            return json.load(f)
    
    def get_last_extracted(self):
        try:
            return utils.mod_time(self.meta_path)
        except OSError, e:
            if e.errno == errno.ENOENT:
                return None
            raise

    def needs_extraction(self, force_older_than=None):

        if self.last_extracted is None:
            log.debug("extracting %s because last_extracted unknown", self.bibcode)
            return True

        if force_older_than:
            if self.last_extracted < force_older_than:
                log.debug("extracting %s because last_extracted %s is older than %s", 
                          self.bibcode, self.last_extracted, force_older_than)
                return True

        meta = self.get_meta()
        if 'ft_source' not in meta:
            log.debug("extracting %s because no 'ft_source' found in meta", self.bibcode)
            return True
        
        if meta['ft_source'] != self.ft_source:
            log.debug("extracting %s because ft_source has changed from %s to %s", 
                      self.bibcode, meta['ft_source'], self.ft_source)
            return True

        # check freshness of fulltext source
        if self.content_is_stale():
            log.debug("extracting %s because meta is older than source", self.bibcode)
            return True

        log.debug("%s does not need extracting", self.bibcode)
        return False
    
    def content_is_stale(self):
        raise NotImplementedError()
    
    def load_source(self):
        raise NotImplementedError()
    
    def get_contents(self):
        return NotImplementedError()

    def write_contents(self, field, text):
        cpath = self.content_path(field)
        log.debug("writing %s contents to %s", field, cpath)
        self.write_file(cpath, text.encode('utf-8'))
    
    def write_meta(self, meta):
        log.debug("writing %s meta to %s", self.bibcode, self.meta_path)
        self.write_file(self.meta_path, json.dumps(meta))
        
    def write_file(self, path, contents):
        """
        use the basic write-replace method 
        """
        if self.dry_run:
            return
        with NamedTemporaryFile('w', dir=self.extract_dir, delete=False) as tf:
            try:
                tf.write(contents)
            except:
                tf.close()
                os.unlink(tf.name)
                raise
            tempname = tf.name
        os.rename(tempname, path)
        
    def extract(self, clobber=False):
        
        if not os.path.exists(self.meta_path):
            log.debug("initializing meta path for %s", self.bibcode)
            meta = self.init_path()
        else:
            meta = self.get_meta()
            
        log.debug("extracting contents of %s from %s", self.bibcode, self.ft_source)
        contents = self.get_contents()
        if contents is None or not len(contents):
            log.debug("Nothing extracted")
            return
        
        record_updated = False
        try:
            for field, text in contents.items():
                
                if not len(text) and not clobber:
                    log.debug("%s contents empty; not writing to disk" % field)
                    continue
                self.write_contents(field, text)
                record_updated = True
            if record_updated:
                if 'index_date' not in meta:
                    meta['index_date'] = datetime.utcnow().isoformat() + 'Z'
                # these values might have changed
                meta['ft_source'] = self.ft_source
                meta['provider'] = self.provider
                self.write_meta(meta)
        except Exception, e:
            log.error("Error writing extracted contents to disk for %s: %s", self.bibcode, str(e))
            raise
        
        return record_updated
    
class HttpExtractor(Extractor):

    def content_is_stale(self):
        self.load_source(only_if_modified = True)
        return self.source_loaded

    def load_source(self, only_if_modified = False):
        """fetch fulltext over http, optionally
        using an if-modified-since header to control
        re-extraction of unchanged content
        """
        req_headers = {'User-Agent': 'ADSClient', 'Accept': 'text/plain'}

        if only_if_modified:
            if self.last_extracted is not None:
                last_extracted_str = self.last_extracted.strftime('%a, %d %b %Y %H:%M:%S %Z')
                log.debug("setting if-modified-since: %s" % last_extracted_str)
                req_headers['If-Modified-Since'] = last_extracted_str

        http = httplib2.Http()
        (resp_headers, resp) = http.request(
            self.ft_source, method="GET", 
            headers=req_headers)

        if resp_headers['status'] == '200':
            self.source_content = resp
            self.source_loaded = True
        elif resp_headers['status'] != '304':
            log.error("http response status: %s" % resp_headers['status'])
            raise FulltextSourceNotFound("no content found at %s" % self.ft_source)

    def get_contents(self):
        if not self.source_loaded:
            self.load_source()

        return { 'fulltext' : utils.text_cleanup(self.source_content, translate=True, decode=True) }

class FileBasedExtractor(Extractor):

    def content_is_stale(self):
        """
        compares the mod time of the record's meta file vs. the mod time
        of the source
        """
        if not os.path.exists(self.ft_source):
            raise FulltextSourceNotFound("no source file found for %s at %s" % (self.bibcode, self.ft_source))
        
        offset = datetime.utcnow() - datetime.now()
        source_mtime = utils.mod_time(self.ft_source) + offset
        log.debug("mtime of %s source file %s: %s", self.bibcode, self.ft_source, source_mtime)

        if not self.last_extracted or source_mtime > self.last_extracted:
            return True
        return False

class PlainTextExtractor(FileBasedExtractor):
    
    def load_source(self):

        with open(self.ft_source, 'r') as f:
            self.source_content = f.read()
            
        self.source_loaded = True

    def get_contents(self):

        if not self.source_loaded:
            self.load_source()

        return { 'fulltext' : utils.text_cleanup(self.source_content, translate=True, decode=True) }

class PdfExtractor(FileBasedExtractor):
    
    def _on_response(self, ch, method, props, body):
        """
        callback method that consumes responses from the extraction workers
        """
        log.debug("got response for %s on %s", self.bibcode, method.routing_key)
        if props.type == 'exception':
            raise PdfExtractException(body)
        self.source_content = body
            
    def load_source(self):
        import pika 
        
        self.channel = utils.rabbitmq_channel()
        res = self.channel.queue_declare(auto_delete=True)
        callback_queue = res.method.queue
        log.debug("created callback_queue: %s" % callback_queue)
        self._consumer_tag = self.channel.basic_consume(self._on_response, no_ack=True, queue=callback_queue)
        
        if 'RABBITMQ_PDF_QUEUE' in os.environ:
            pdf_queue_name = os.environ['RABBITMQ_PDF_QUEUE']
        else:
            pdf_queue_name = config['RABBITMQ_PDF_QUEUE']
            
        log.debug("queueing %s for pdf extraction on %s", self.bibcode, pdf_queue_name)
        
        msg = json.dumps({ 'bibcode': self.bibcode, 'ft_source': self.ft_source })
        properties = pika.BasicProperties(reply_to=callback_queue)
        self.channel.basic_publish(exchange='',
                              routing_key=pdf_queue_name,
                              properties=properties,
                              body=msg)
        log.debug("waiting for responses...")
        while self.source_content is None:
            self.channel.connection.process_data_events()
        
    def get_contents(self):

        if not self.source_loaded:
            self.load_source()
        return { 'fulltext' : utils.text_cleanup(self.source_content, translate=True, decode=True) }    
    
class XMLExtractor(FileBasedExtractor):

    def __init__(self, *args):
        FileBasedExtractor.__init__(self, *args)
        self.root = None

    def load_source(self):
        with open(self.ft_source,'r') as f:
            raw_xml = f.read()
            raw_xml = re.sub('(<!-- body|endbody -->)', '', raw_xml)
            raw_xml = convertentities(raw_xml.decode('utf-8', 'ignore'))
            raw_xml = re.sub('<\?CDATA.+?\?>', '', raw_xml)
            self.source_content = raw_xml
        self.source_loaded = True

    def parse_xml(self):
        root = soupparser(self.source_content.encode('utf-8'))
        # strip out the latex stuff (for now)
        for e in root.xpath('//inline-formula'):
            e.getparent().remove(e)
        return root

    def extract_body_content(self, root):
        for path in ['//body','//section[@type="body"]', '//journalarticle-body']:
            log.debug("trying xpath: %s" % path)
            try:
                return root.xpath(path)[0].text_content()
            except IndexError:
                pass
        raise FulltextContentsNotFound("no fulltext found for %s in %s" % (self.bibcode, self.ft_source))

    def extract_ack_content(self, root):
        ack = ""
        for path in ['//ack', '//section[@type="acknowledgments"]', '//subsection[@type="acknowledgement" or @type="acknowledgment"]']:
            log.debug("trying xpath: %s" % path)
            for section in root.xpath(path):
                ack += section.text_content() + "\n"
        return ack

    def get_contents(self):

        if not self.source_loaded:
            self.load_source()

        root = self.parse_xml()
        
        contents = {}
        body = self.extract_body_content(root) 
        contents['fulltext'] = utils.text_cleanup(body)

        ack = self.extract_ack_content(root)
        contents['acknowledgements'] = utils.text_cleanup(ack)

        return contents

class ElsevierExtractor(XMLExtractor):
    
    def parse_xml(self):
        """soupparser doesn't seem to do the right thing for elsevier stuff,
        so use the standard html parser instead
        """
        root = htmlparser(self.source_content.encode('utf-8'))
        return root
    
    def extract_body_content(self, root):
        for path in ['//raw-text', '//body']:
            log.debug("trying xpath: %s" % path)
            try:
                return root.xpath(path)[0].text_content()
            except IndexError, e:
                pass
            
        # try using the default parsing
        root = XMLExtractor.parse_xml(self)
        return XMLExtractor.extract_body_content(self, root)
        raise FulltextContentsNotFound("no fulltext found for %s in %s" % (self.bibcode, self.ft_source))

    def extract_ack_content(self, root):
        ack = ""
        for path in ['//acknowledgment']:
            log.debug("trying xpath: %s" % path)
            for section in root.xpath(path):
                ack += section.text_content() + "\n"
        if len(ack) == 0:
            ack = XMLExtractor.extract_ack_content(self, root)
        return ack

class HtmlExtractor(FileBasedExtractor):

    def __init__(self, *args):
        FileBasedExtractor.__init__(self, *args)
        self.main_article = None
        self.table_root_nodes = {}

    def content_is_stale(self):
        """
        compares the mod time of the record's meta file vs. the mod time
        of the each listed source file
        """
        if not self.last_extracted:
            return True
        
        source_files = re.split('\s*,\s*', self.ft_source)
        offset = datetime.utcnow() - datetime.now()
        
        for sf in source_files:
            if not os.path.exists(sf):
                raise FulltextSourceNotFound("no source file found for %s at %s" % (self.bibcode, sf))
            source_mtime = utils.mod_time(sf) + offset
            log.debug("%s source mtime: %s" % (sf, source_mtime))
            if source_mtime > self.last_extracted:
                return True
        return False
    
    def parse_html(self, file):
        html = open(file, 'r').read()
        html = html.decode('utf-8', 'ignore')
        html = convertentities(html)
        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(html), parser)
        return tree.getroot()

    def parse_article_text(self):
        # find the intro node and drop everything before that
        intro = None
        for exp in [
            "//h2[contains(.,'ntroduction')]",
            "//h3[contains(.,'ntroduction')]",
            "//p[contains(., 'Abstract')]",
        ]:
            try:
                intro = self.main_article.xpath(exp)[0]
                break
            except:
                pass

        if intro is None:
            log.debug("Couldn't find intro for %s" % self.bibcode)
        else:
            intro_pos = intro.getparent().index(intro)
            for node in intro.getparent().getchildren()[:intro_pos]:
                node.getparent().remove(node)

        # remove the references
        ref_heading = None
        try:
            ref_heading = self.main_article.xpath("//h2[contains(.,'References')]")[0]
            ul = ref_heading.getnext()
            ul.getparent().remove(ul)
            ref_heading.getparent().remove(ref_heading)
        except:
            log.error("References not removed: %s" % self.bibcode)

        # insert table nodes
        for table_name, table_root_node in self.table_root_nodes.items():
            containing_div_node = None
            try:
                containing_div_node = table_root_node.xpath('//table')[0].getparent()
            except:
                raise Exception("No table found in %s!" % table_name)
            
            nodes_in_article = self.main_article.xpath('//a[contains(@href,"%s")]' % table_name)
            if nodes_in_article:
                parent = nodes_in_article[0].getparent()
                parent.replace(nodes_in_article[0], containing_div_node)
                [n.getparent().remove(n) for n in nodes_in_article[1:]]

        try:
            head = self.main_article.xpath('//head')[0]
            self.main_article.remove(head)
        except:
            pass
        return " ".join([t for t in self.main_article.itertext() if t and not t.isspace()])

    def load_source(self):
        source_files = re.split('\s*,\s*', self.ft_source)
        source_files.reverse()
        self.main_article = self.parse_html(source_files.pop())

        for table_src in filter(lambda x: x.find('table') != -1, source_files):
            table_name = basename(table_src)
            self.table_root_nodes[table_name] = self.parse_html(table_src)

        self.source_loaded = True

    def get_contents(self):

        if not self.source_loaded:
            self.load_source()

        body = self.parse_article_text() 
        return { 'fulltext' : utils.text_cleanup(body) }


    
