from bs4 import BeautifulSoup
import requests
import pandas as pd
import itertools
import yfinance as yf

import hopsworks

from datetime import datetime, timedelta

## The code to fetch the news article data from various websites
def get_stock_price(stock_sym, start_time, end_time):
  company = 'APPLE'
  if stock_sym == 'AMAZ':
    company = 'AMAZON'
  if stock_sym == 'META':
    company = 'META'
  market_df = yf.download(stock_sym, start=start_time, end=end_time)
  market_df = market_df.reset_index(level=0)
  market_df.columns = market_df.columns.str.lower()
  market_df.rename(columns={'adj close': 'adj_close'}, inplace=True)
  market_df.insert(0, 'name', company)
  market_df['date'] = pd.to_datetime(market_df.date).dt.tz_localize(None)
  return market_df

## Uploading stocks related data from hopworks
def time_2_datetime(i):
    get_date = datetime.fromtimestamp(i / 1000)
    return get_date

def get_stock_price_from_hopsworks():
  project = hopsworks.login()
  fs = project.get_feature_store() 
  stock_fg = fs.get_feature_group(name="stocks_fg", version=1)  
  query = stock_fg.select_all()
  stock_df = query.read()
  stock_df['date'] = stock_df['date'].apply(time_2_datetime)
  stock_df = stock_df.sort_values(by='date')
  return stock_df.head(1)

## here we are Scraping stock news 
def get_articles_urls(company,startpg, endpg):
  urls=[]
  for pg in range(startpg, endpg):
    if pg % 100 == 0:
      print(pg)
    url = f"https://www.investing.com/equities/{company}-inc-news/{page}"
    page=requests.get(url)
    soup=BeautifulSoup(page.text,'html.parser')
    for elt in soup.find_all('div',attrs={'class':'mediumTitle1'})[1].find_all('article'):
        urls.append('https://www.investing.com/'+elt.find('a')['href'])
  return list(itertools.filterfalse(lambda x: x.startswith('https://www.investing.com//pro/offers'), urls))

def scrape_news(urls, df, company):
  for url in urls:
    page = requests.get(url)
    soup=BeautifulSoup(page.text,'html.parser')
    if type(soup.find('h1',attrs={'class':'articleHeader'})) is type(None):
      print(url)
      continue
    Title=soup.find('h1',attrs={'class':'articleHeader'}).text.strip()
    Date=soup.find('div',attrs={'class':'contentSectionDetails'}).find("span").text.strip()
    Article=' '.join([x.get_text() for x in soup.find('div',attrs={'class':'WYSIWYG articlePage'}).find_all("p")]).replace('Position added successfully to:','').strip()
    tmpdic = {'ticker': company, 'publish_date': Date, 'title': Title, 'body_text': Article, 'url': url}
    df=df.append(pd.DataFrame(tmpdic, index=[0]))
  return df

## We Fetched stock news from hopsworks:
def get_news_from_hopsworks():
  project = hopsworks.login()
  fs = project.get_feature_store() 
  news_fg = fs.get_feature_group(name="market_news_fg_for_three", version=1)  
  # try: 
  #   feature_view = fs.get_feature_view(name="market_news", version=1)
  # except:
  #   news_fg = fs.get_feature_group(name="market_news_fg", version=1)
  #   query = news_fg.select_all()
  #   feature_view = fs.create_feature_view(name="market_news",
  #                                         version=1,
  #                                         description="Read from market_news_fg",
  #                                         query=query)
  query = news_fg.select_all()
  return query.read()

## here we Fetched history prediction plot
def get_history_plot_from_hopsworks(ticker):
  project = hopsworks.login()
  dataset_api = project.get_dataset_api()
  if ticker == 'AAPL':
    dataset_api.download("Resources/images/apple_stock_prediction.png", overwrite=True)
  if ticker == 'AMZN':
    dataset_api.download("Resources/images/amazon_stock_prediction.png", overwrite=True)
  else:
    dataset_api.download("Resources/images/meta_stock_prediction.png", overwrite=True)
  return
 
## here we Formalize the date column
def remove_parentheses(k):
  if '(' in k:
    return k[k.find("(")+1:k.find(")")]
  else:
      return k
def change_date_format(df):
  if df['publish_date'].dtype == object:
    df.publish_date = df.publish_date.apply(remove_parentheses)
    df['publish_date'] = pd.to_datetime(df['publish_date'], format='%b %d, %Y %I:%M%p ET')
  return df

def select_oneday_news(df, day):
  df_copy = df.copy()
  df['date'] = change_date_format(df_copy)['publish_date']
  df['date'] = df['date'].apply(lambda x : x.date())
  df = df.loc[df['date'] == day.date()]
  df = df.drop('date', axis=1)
  return df

