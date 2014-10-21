import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from psql_models import Metrics


class Session:

  def __init__(self,DATABASE_URI='postgresql+psycopg2://metrics:metrics@localhost:5432/metrics'):

    self.DATABASE_URI = DATABASE_URI
    self.engine = create_engine(DATABASE_URI)
    
    self.Base = declarative_base()
    self.Base.metadata.create_all(self.engine)

    self.connection = sessionmaker(bind=self.engine)()

  def save_metrics_record(self,record):
    #Very domain specific; strong assumption of the incoming data's schema

    #example data:
    # {'_id': '1920ApJ....51....4D',
    #                                'refereed': True,
    #                                'rn_citations': 0.070302403721891962,
    #                                'rn_citation_data': [{'bibcode':u'1983ARA&A..21..373O','ref_norm':0.018867924528301886}, {'bibcode':u'2000JOptB...2..534W', 'ref_norm': 0.018867924528301886}, {'bibcode':u'2000PhRvL..84.2094A', 'ref_norm': 0.013698630136986301}, {'bibcode':u'2001AJ....122..308G','ref_norm': 0.018867924528301886}],
    #                                'downloads': [0, 0, 0, 5, 3, 3, 2, 6, 1, 8, 7, 2, 7, 3, 2, 0, 4, 5],
    #                                'reads': [0, 0, 0, 5, 4, 3, 3, 6, 1, 8, 12, 4, 7, 3, 2, 2, 8, 0],
    #                                'an_citations': 0.052631578947368418,
    #                                'refereed_citation_num': 4,
    #                                'citation_num': 5,
    #                                'citations': [u'1983ARA&A..21..373O', u'2000JOptB...2..534W', u'2000PhRvL..84.2094A', u'2001AJ....122..308G', u'2011foobar........X'],
    #                                'refereed_citations': [u'1983ARA&A..21..373O', u'2000JOptB...2..534W', u'2000PhRvL..84.2094A', u'2001AJ....122..308G'],
    #                                'author_num': 1,
    #                                'an_refereed_citations': 0.042105263157894736,
    #                                'rn_citations_hist': {u'1983': 0.018867924528301886,
    #                                                      u'2000': 0.089170328250193845,
    #                                                      u'2001': 0.070302403721891962}
    #                                }  

    record['bibcode'] = record['_id']
    del record['_id']
    if '_digest' in record:
      del record['_digest']
    record['modtime'] = datetime.datetime.now()

    current = self.connection.query(Metrics).filter(Metrics.bibcode==record['bibcode']).one()
    if current:
      excluded_fields = ['modtime']
      if dict((k,v) for k,v in current[0].iteritems() if k not in excluded_fields)==dict((k,v) for k,v in record.iteritems() if k not in excluded_fields):
        #Record is the same as the current one: no-op
        return
      for k,v in record.iteritems():
        current[0].__setattr__(k,v)
    else:
      self.connection.add(Metrics(**record))
    self.connection.commit()

  def close(self):
    self.connection.close()
