import hopsworks
import joblib
import math
from sklearn.preprocessing import MinMaxScaler 
import numpy as np
from datetime import timedelta, datetime





def model(ticker):
    project = hopsworks.login() 

    # import data
    fs = project.get_feature_store() 
    feature_group = fs.get_feature_group(
        name = 'final_data_for_prediction')

    data = feature_group.select_all().read()
    data = data.sort_values(by='date')

    last_date = data['date'].values[-1]
    last_date = datetime.fromtimestamp(int(int(last_date) / 1000))
    date = last_date.date() + timedelta(days=1)

    data = data.set_index('date')
    if ticker == 'AAPL':
        data = data.loc[data['name'] == 'APPLE']
    elif ticker == 'AMZN':
        data = data.loc[data['name'] == 'AMAZON']
    else:
        data = data.loc[data['name'] == 'META']
    data.drop(['name', 'price_move'], axis=1, inplace=True)

    # scaling data
    prices = data[['close','neg','neu','pos','compound']]
    scaler = MinMaxScaler(feature_range=(0,1))
    scaled_data = scaler.fit_transform(prices)

    prediction_list = scaled_data[-60:]

    x = []
    x.append(prediction_list[-60:])
    x = np.array(x)

    # import model
    mr = project.get_model_registry()
    if ticker == 'AAPL':
        remote_model = mr.get_model("LSTM_Apple", version=1)
        model_dir = remote_model.download()
        remote_model = joblib.load(model_dir + "/apple_model.pkl")
    elif ticker == 'AMZN':
        remote_model = mr.get_model("LSTM_Amazon", version=1)
        model_dir = remote_model.download()
        remote_model = joblib.load(model_dir + "/amazon_model.pkl")
    else:
        remote_model = mr.get_model("LSTM_Meta", version=1)
        model_dir = remote_model.download()
        remote_model = joblib.load(model_dir + "/meta_model.pkl")

    # predict
    out = remote_model.predict(x)
    B=np.hstack((out,scaled_data[ : 1,1:]))
    out = scaler.inverse_transform(B)[0,0]
    return date, out 