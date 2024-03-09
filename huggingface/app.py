from data_loader_functions import *
from sentiment_analysis import *
from stock_prediction import *

from datetime import datetime

import pandas as pd
import streamlit as st

from bs4 import BeautifulSoup
import requests

st.set_page_config(layout="wide")

st.title("Stock Prediction via News Sentiments")


left_column, right_column = st.columns(2)

with left_column:

    all_tickers = {
                "Apple":"AAPL", 
                "Amazon":"AMZN",
                "Meta":"META",
                }

    st.subheader("Select Stock to Analyze")
    option_name = st.selectbox('Choose a stock:', all_tickers.keys())
    option_ticker = all_tickers[option_name]
    'Your selection: ', option_name, "(",option_ticker,")"

    st.subheader("Vader-based Sentiment Analysis")

    with st.spinner("Connecting with Hopsworks..."):
        df = sentiment_analysis(option_name, datetime(2023, 1, 5))
        df_copy = df.copy()
        df_copy = df_copy.set_index('publish_date')
        st.table(df_copy.drop(['body_text', 'text_w_puncts', 'text_tokenized', 'text_w_stopwords', 'text_lemmatized', 'text_stemmed', 'text_processed', 'predicted_class'], axis=1))
        daily_df = aggregate_by_date(df)
        "Current sentiment:", daily_df.iloc[0]['compound']
    
with right_column:

    st.subheader("Latest Stock Price")

    with st.spinner('Loading stock data from Hopsworks...'):
        stock_df = get_stock_price_from_hopsworks(option_name)
        st.table(stock_df)

    st.subheader("LSTM-based stock price prediction model")

    get_history_plot_from_hopsworks(option_ticker)
    st.image(option_name.lower() + "_stock_prediction.png", caption="Latest Model Performance")

    with st.spinner("Loading LSTM model from Hopsworks.."):
        date, value = model(option_ticker)
        "The predicted stock value on ", date, "is", value