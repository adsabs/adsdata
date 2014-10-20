import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql

from psql_models import Metrics

Base = declarative_base()
DATABASE_URI = 'postgresql+psycopg2://metrics:metrics@localhost:5432/metrics'

def init():
  engine = create_engine(DATABASE_URI)
 
  # Create all tables in the engine. This is equivalent to "Create Table"
  # statements in raw SQL.
  Base.metadata.create_all(engine)

  DBSession = sessionmaker(bind=engine)
  session = DBSession()
  return session

def save(record):

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

  session = init()
  
  #Manipulate the data a little bit
  record['bibcode'] = record['_id']
  del record[_id]

  record['modtime'] = datetime.datetime.now()

  session.save(**record)
  session.commit()
  session.close()