import pandas as pd


def transform_weather_data(df, geocoded_city):

    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    return df