# Linear Algebra and DataFrames
import numpy as np 
import pandas as pd 

# Visualization libraries
import seaborn as sns
sns.set_style("whitegrid")

# NLP Preprocessing and Basic tools
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re
import string
from emoji import demojize

from data_loader_functions import *


## Crawl down the news from investing.com
def news_scraping(company):
    # scrape news
    urls = get_articles_urls('apple-computer', 1, 3)
    if company == 'Amazon':
        urls = get_articles_urls('amazon-com', 1, 3)
    elif company == 'Meta':
        urls = get_articles_urls('facebook', 1, 3)
    articles_df = pd.DataFrame({'ticker':[],
                                    'publish_date':[],
                                    'title': [],
                                    'body_text': [],
                                    'url':[]})
    articles_df=scrape_news(urls, articles_df, company)

    # Checking the data for duplicates
    articles_df[articles_df.duplicated('body_text',keep=False)].sort_values('body_text')

    # Dropping all duplicates
    articles_df.drop_duplicates(('body_text'), inplace=True)
    return articles_df


## Fetch news from hopsworks
def fetching_news(company):
    articles_df = get_news_from_hopsworks()
    articles_df.loc[articles_df['ticker'] == company]
    articles_df['publish_date'] = articles_df['publish_date'].apply(time_2_datetime)
    return articles_df


## NLP Processes
# Remove mentions
def remove_urls(text):
    return re.sub(r'https?://\S+|www\.\S_+', '', text)

def remove_usernames_ressource(text):
    text_split = text.split("-",1)
    if len(text_split)>1:
        text=text_split[1]
    text = re.sub(r'@[A-Za-z0-9_]+', ' ', text)
    return text

# Remove hashtags
def remove_hashtags(text):
    return re.sub("#[A-Za-z0-9_]+"," ", text)

# Remove punctuations
def remove_punctuation(text, punc_list):
    return text.translate(str.maketrans('', '', punc_list))

# Convert emojis to texts
def convert_emojis(text):
    return demojize(text).replace(":","")

# Apply the previous functions
def full_preprocessing(text):
    """
    @param    text (str): a string to be processed.
    @return   text (Str): the processed string.
    """
    punc_list = string.punctuation

    # Remove non-ascii words
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)

    # Replace '&amp;' with 'and'
    text = re.sub(r'&amp;', 'and', text)

    # Remove trailing whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    text = remove_urls(text)
    text = remove_usernames_ressource(text)
    text = remove_hashtags(text)
    text = remove_punctuation(text, punc_list)
    text = convert_emojis(text)

    return text.lower()

# Trasform text to tokens (separated words)
def tokenize(text) :
    text = text.split()
    return text

# Remove stopwords
def remove_stopwords(text, stop_words):
    words_to_keep = ["not","no","nor"]
    stopword = [elem for elem in stop_words if not elem in words_to_keep]
    text = [w.lower() for w in text if not w.lower() in stopword]
    return text

# Lemmatization
def lemmatize(text, wn):
    text = [wn.lemmatize(word) for word in text]
    return text

# Stemming
def stemming(text, ps, ls):
    text = [ps.stem(word) for word in text]
    text = [ls.stem(word) for word in text]
    return text

def full_processing(df):
    stop_words = stopwords.words('english')
    wn = nltk.WordNetLemmatizer()
    ps = nltk.PorterStemmer()
    ls = nltk.LancasterStemmer()

    df["text_W_puncts"] =df["body_text"].apply(lambda x: full_preprocessing(x))
    df["text_tokenized"] = df["text_W_puncts"].apply(lambda x: tokenize(x))
    df["text_W_stopwords"] = df["text_tokenized"].apply(lambda x: remove_stopwords(x, stop_words))
    df["text_lemmatized"] = df["text_W_stopwords"].apply(lambda x: lemmatize(x, wn))
    df["text_stemmed"] = df["text_lemmatized"].apply(lambda x: stemming(x, ps, ls))
    df["text_processed"] = df["text_stemmed"].apply(lambda x: ' '.join(str(e) for e in x))
    
    return df
    

def nlp_processing(articles_df):
    news=articles_df[['body_text','publish_date','title']]
    # Number of mentions, hashtags, urls
    cnt_1, cnt_2, cnt_3 = 0, 0, 0
    max_len, min_len, mean_len = -float("inf"), float("inf"), 0
    for row in news.values:
        text = row[0]  # 0 for text content
        if "@" in text:
            cnt_1 += 1
        if "#" in text:
            cnt_2 += 1
        if 'http' or 'www' in text:
            cnt_3 += 1
        if len(text) < min_len:
            min_len = len(text)
        if len(text) > max_len:
            max_len = len(text)

        mean_len += len(text)
        
    mean_len /= len(articles_df)
    
    nltk.download('stopwords')
    nltk.download('wordnet')
    nltk.download('omw-1.4')

    articles_processed = full_processing(articles_df)

    return articles_processed

    
## Vader Sentiment
def predicted_label(x):
  if x<=-0.5:
    return 0
  elif x>=0.5:
    return 2
  else:
    return 1

def score_Vader(df,analyzer):
  df['neg'] = df['text_processed'].apply(lambda x:analyzer.polarity_scores(x)['neg'])
  df['neu'] = df['text_processed'].apply(lambda x:analyzer.polarity_scores(x)['neu'])
  df['pos'] = df['text_processed'].apply(lambda x:analyzer.polarity_scores(x)['pos'])
  df['compound'] = df['text_processed'].apply(lambda x:analyzer.polarity_scores(x)['compound'])
  
  df['predicted_class'] = df['compound'].map(predicted_label) 
  return df

def vader_sentiment(articles_processed):
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    articles_processed=score_Vader(articles_processed, analyzer)
    return articles_processed

def sentiment_analysis(company, day):
    articles_df = fetching_news(company)
    articles_df = select_oneday_news(articles_df, day)
    articles_df = articles_df.loc[articles_df['ticker'] == company.upper()]
    # articles_processed = nlp_processing(articles_df)
    # articles_sentimentalized = vader_sentiment(articles_processed)
    return articles_df

## Aggregate News Sentiments Each Day
def aggregate_by_date(articles_sentiments):
    articles_sentiments = change_date_format(articles_sentiments)
    keep_columns = ['ticker', 'publish_date', 'neg', 'neu', 'pos', 'compound']
    sentiment_df = articles_sentiments[keep_columns]
    daily_sentiment = sentiment_df.groupby([sentiment_df['publish_date'].dt.date, 'ticker']).agg({'neg': 'mean', 'neu': 'mean', 'pos': 'mean', 'compound': 'mean'}).reset_index()
    return daily_sentiment


