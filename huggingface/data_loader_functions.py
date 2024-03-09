from bs4 import BeautifulSoup
import requests
import pandas as pd
import itertools
import yfinance as yf

import hopsworks

from datetime import datetime, timedelta

## Fetch stock price data from Yahoo Finance
def get_stock_price(ticker, start_date, end_date):
  company = 'APPLE'
  if ticker == 'AMAZ':
    company = 'AMAZON'
  elif ticker == 'META':
    company = 'META'
  stock_df = yf.download(ticker, start=start_date, end=end_date)
  stock_df = stock_df.reset_index(level=0)
  stock_df.columns = stock_df.columns.str.lower()
  stock_df.rename(columns={'adj close': 'adj_close'}, inplace=True)
  stock_df.insert(0, 'name', company)
  stock_df['date'] = pd.to_datetime(stock_df.date).dt.tz_localize(None)
  return stock_df

## Fetch stock news from hopsworks
def time_2_datetime(x):
    
    dt_obj = datetime.fromtimestamp(x / 1000)
    return dt_obj

def get_stock_price_from_hopsworks(name):
  project = hopsworks.login()
  fs = project.get_feature_store() 
  stock_fg = fs.get_feature_group(name="stocks_fg", version=1)  
  query = stock_fg.select_all()
  stock_df = query.read()
  stock_df = stock_df.loc[stock_df['name'] == name.upper()]
  stock_df['date'] = stock_df['date'].apply(time_2_datetime)
  stock_df = stock_df.sort_values(by='date')
  return stock_df.head(1)

## Scrape stock news from investing.com
def get_articles_urls(company,startpage, endpage):
  urls=[]
  for page in range(startpage, endpage):
    if page % 100 == 0:
      print(page)
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

## Fetch stock news from hopsworks
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

## Fetch history prediction plot
def get_history_plot_from_hopsworks(ticker):
  project = hopsworks.login()
  dataset_api = project.get_dataset_api()
  if ticker == 'AAPL':
    dataset_api.download("Resources/images/apple_stock_prediction.png", overwrite=True)
  elif ticker == 'AMZN':
    dataset_api.download("Resources/images/amazon_stock_prediction.png", overwrite=True)
  else:
    dataset_api.download("Resources/images/meta_stock_prediction.png", overwrite=True)
  return

## Formalize the date column
def remove_parentheses(s):
  if '(' in s:
    return s[s.find("(")+1:s.find(")")]
  else:
      return s
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

