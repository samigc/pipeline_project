import argparse
import logging
logging.basicConfig(level= logging.INFO)
logger = logging.getLogger(__name__)
import pandas as pd
from base import Base, engine, Session
from article import Article

def main(filename):
 #genera un schema
 Base.metadata.create_all(engine)
 session = Session()
 articles = pd.read_csv(filename)
 for index, row in articles.iterrows():
  logger.info('Loading article uid {} into DB'.format(row['uid']))
  article = Article(row['uid'],
  row['body'],
  row['host'],
  row['newspaper_uid'],
  row['n_tokens_title'],
  row['n_tokens_body'],
  row['title'],
  row['url'])

  session.add(article)
  session.commit()
  session.close()

if __name__=='__main__':
 parser = argparse.ArgumentParser()
 parser.add_argument('filename',
 help= ' the file you want to load into the db',
 type= str)

 args = parser.parse_args()
 main(args.filename)
