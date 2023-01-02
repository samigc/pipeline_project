import argparse 
import hashlib
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import nltk 
from nltk.corpus import stopwords


import pandas as pd
from urllib.parse import urlparse



def main(filename):
 logger.info('starting cleaning process')

 df = _read_data(filename)
 newspaper_uid = _extract_newspaper_uid(filename)
 df = _add_newspaper_uid_column(df, newspaper_uid)
 df = _extract_host(df)
 df = _fill_missing_titles(df)
 df = _generate_uids_for_rows(df)
 df = _remove_new_lines_from_body(df)
 df= _tokenize_column(df,'title')
 df= _tokenize_column(df,'body')
 df = _remove_duplicate_entries(df,'title')
 df = drop_rows_with_missing_values(df)
 _save_data(df,filename)

 return df


def _add_newspaper_uid_column(df,newspaper_uid):
 logger.info('filling news paper uid column with {}'.format(newspaper_uid))
 df['newspaper_uid'] = newspaper_uid

 return df


def _extract_host(df):
 logger.info('Extracting host from url')
 df['host'] = df['url'].apply(lambda url:urlparse(url).netloc)

 return df


def _extract_newspaper_uid(filename):
 logger.info('Extracting Newspaper uid')
 newspaper_uid = filename.split('_')[0]
 logger.info('Newspaper uid detected {}'.format(newspaper_uid))

 return newspaper_uid


def _read_data(filename):
 logger.info('Reading file {}'.format(filename))

 return pd.read_csv(filename)


def _fill_missing_titles(df):
 logger.info('Filling missing titles')
 missing_titles_mask = df['title'].isna()
 missing_titles = (df[missing_titles_mask]['url']
  .str.extract(r'(?P<missing_titles>[^/]+)$')
  .applymap(lambda title:title.replace('-',' '))
  .applymap(lambda final_title: final_title.capitalize()))
 df.loc[missing_titles_mask,'title'] = missing_titles.loc[:,'missing_titles']

 return df


def _generate_uids_for_rows(df):
 logger.info('generate uids for each row')
 uids = (df
 .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
 .apply(lambda hash_object:hash_object.hexdigest())
 )
 df['uid'] = uids

 return df.set_index('uid')


def _remove_new_lines_from_body(df):
 logger.info('Removing new lines from the body')
 stripped_body = (df
 .apply(lambda row: row['body'], axis = 1 )
 .apply(lambda body: body.replace('\n',' '))
 .apply(lambda body: body.replace('\r',' '))
 )
 df['body'] = stripped_body

 return df


def _tokenize_column(df, column_name):
 logger.info('calculating number of unique tokens in {}'.format(column_name))
 stop_words = set(stopwords.words('portuguese'))
 n_tokens = (df
 .dropna()
 .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
 .apply(lambda tokens: list(filter(lambda token:token.isalpha(), tokens)))
 .apply(lambda tokens: list(filter(lambda token:token.lower(),tokens)))
 .apply(lambda word_list:list(filter(lambda word:word not in stop_words,word_list)))
 .apply(lambda valid_word_list:len(valid_word_list))
 ) 
 df['n_tokens_'+column_name] = n_tokens

 return df


def _remove_duplicate_entries(df,column_name):
 logger.info('removing duplicated entries')
 df.drop_duplicates(subset= column_name, keep='first', inplace =True)

 return df


def drop_rows_with_missing_values(df):
 logger.info('drop rows with missing values')
 return df.dropna()


def _save_data(df,file_name):
 clean_file_name = 'clean_{}'.format(file_name)
 logger.info('saving data at location:{}'.format(file_name))
 df.to_csv(clean_file_name)



if __name__ == '__main__':
 parser = argparse.ArgumentParser()
 parser.add_argument('filename',
  help = ' The path to the dirty data',
  type = str)
 args = parser.parse_args()

 df = main(args.filename)
 #df.to_csv('news_clean_df.csv', encoding= 'utf-8', sep=';')
 print(df)
