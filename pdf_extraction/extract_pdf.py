'''
Created on Jul 22, 2014

@author: jluker
'''

import os
import sys
from os.path import dirname, abspath
sys.path.insert(0, dirname(dirname(abspath(__file__))))

import json
import logging
from adsdata import utils
from optparse import OptionParser
from traceback import format_exc

import java.io
from java.lang import String
from java.lang import Thread
from java.util import HashMap
from java.util.concurrent import Executors, TimeUnit
from java.util.concurrent import Callable
from java.lang import InterruptedException
from org.apache.pdfbox.pdfparser import PDFParser
from org.apache.pdfbox.pdmodel import PDDocument
from org.apache.pdfbox.util import PDFTextStripper, TextNormalize
from com.rabbitmq.client import ConnectionFactory
from com.rabbitmq.client import Connection
from com.rabbitmq.client import Channel
from com.rabbitmq.client import QueueingConsumer
from com.rabbitmq.client.AMQP import BasicProperties

config = utils.load_config()

class PdfExtractor(Callable):

    def __init__(self, channel, opts):
        self.channel = channel
        self.opts = opts
        self.consumer = QueueingConsumer(channel)
    
    def call(self):
        log = logging.getLogger()
        self.channel.basicConsume(opts.queue_name, False, self.consumer)
        log.info("Awaiting pdf extraction tasks on %s...", opts.queue_name)
        while True:
            delivery = self.consumer.nextDelivery()
            props = delivery.getProperties()
            task = String(delivery.getBody())
            reply_props = BasicProperties.Builder().build()
            try:
                task_data = json.loads(str(task))
                log.debug("got task: %s", str(task_data))
                resp = self.extract(task_data['bibcode'], task_data['ft_source'])
                self.channel.basicPublish("", props.getReplyTo(), reply_props, resp)
            except Exception, e:
                msg = format_exc()
                log.debug("returning error response: %s" % str(msg))
                error_props = BasicProperties.Builder().type("exception").build()
                pub = self.channel.basicPublish("", props.getReplyTo(), error_props, msg)
            finally:
                log.debug("acknowledging task processed")
                self.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), False)
            
        return self

    def extract(self, bibcode, ft_source):

        log = logging.getLogger()
        log.debug("extracting text from %s, %s", bibcode, ft_source)
        f = java.io.File(ft_source)
        parsedText = ""

        try:
            fis = java.io.FileInputStream(f)
            parser = PDFParser(fis)
        except java.io.IOException, e:
            log.error("Unable to create PDFParser for %s from %s: %s", bibcode, ft_source, e.getMessage())
            raise

        pdDoc = None
        try:
            parser.parse()
            cosDoc = parser.getDocument()
            pdfStripper = PDFTextStripper() #"utf-8")
            pdDoc = PDDocument(cosDoc)
            parsedText = pdfStripper.getText(pdDoc)
        except: # Exception, e:
            log.error("Exception occurred while parsing %s: %s", ft_source, str(sys.exc_info()))
            raise
        finally:
            pdDoc.close()

        try:
            normalizer = TextNormalize("utf8")
            parsedText = normalizer.normalizeDiac(parsedText)
            parsedText = normalizer.normalizePres(parsedText)
            parsedText = parsedText.replace("\n", "")
        except: # Exception, e:
            log.error("Exception occurred during normalization of %s: %s", ft_source, str(sys.exc_info()))
            raise

        return parsedText

def main(opts):
        
    # set up our channel
    conn_factory = ConnectionFactory()
    conn_factory.setUri(config['RABBITMQ_URI'])
    conn = conn_factory.newConnection()
    channel = conn.createChannel()
    channel.queueDeclare(opts.queue_name, False, False, False, None)
    channel.basicQos(1); # tells the channel we're only going to deliver one response before req acknowledgement 
    
    workers = [PdfExtractor(channel, opts) for i in xrange(opts.workers)]    
    
    log.info("creating pool with %d threads" % opts.workers)
    tpool = Executors.newFixedThreadPool(opts.workers)

    log.info("executing threads")
    futures = tpool.invokeAll(workers)

    log.info("shutting down thread pool")
    tpool.shutdown()

    try:
        if not tpool.awaitTermination(5, TimeUnit.SECONDS):
            log.info("thread pool not shutting down; trying again")
            tpool.shutdownNow()
            if not tpool.awaitTermination(5, TimeUnit.SECONDS):
                log.error("Pool did not terminate")
    except InterruptedException:
        log.info("exception during thread pool shutdown; trying again")
        tpool.shutdownNow()
        Thread.currentThread().interrupt()    
        
if __name__ == '__main__':
    
    op = OptionParser()
    op.set_usage("usage: pdf_jenerate.py [options] ")
    op.add_option('-v','--verbose', dest='verbose', action='store_true',
        help='write log output to stdout', default=False)
    op.add_option('-d','--debug', dest='debug', action='store_true',
        help='include debugging info in log output', default=False)
    op.add_option('-w','--workers', dest='workers', action='store', type=int,
        help='number of workers to use for extracting', default=4)
    op.add_option('-q','--queue_name', dest='queue_name', action='store', type=str,
        help='consume tasks from this queue', default="extract_pdf")

    opts, args = op.parse_args()
    
    log = utils.init_logging(utils.base_dir(), __file__, None, opts.verbose, opts.debug)
    if opts.debug:
        log.setLevel(logging.DEBUG)
        
    main(opts)
