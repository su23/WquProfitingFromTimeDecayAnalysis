from math import isnan
from functools import total_ordering
import calendar
import zipfile
import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from datetime import datetime
import datetime
import warnings

warnings.filterwarnings('ignore')


def load(asset: str, option_type: str = "call"):
    data = pd.read_csv(f'{asset}.csv', sep=",").filter(['UnderlyingPrice', 'Type', 'Expiration',
                                                        'DataDate', 'Strike', 'Last', 'Bid', 'Ask', 'Volume',
                                                        'OpenInterest', 'IV', 'Delta', 'Gamma',
                                                        'Theta', 'Vega'])
    data['Expiration'] = pd.to_datetime(data['Expiration'])
    data['DataDate'] = pd.to_datetime(data['DataDate'])
    df_typed = data.loc[data['Type'] == option_type]
    return df_typed


def select_closest_dn_option_max_expiration_days(df, delta: float = -0.5):
    df['Days'] = (df['Expiration'] - df['DataDate']).astype('timedelta64[D]').astype(int)
    df2 = df.groupby('DataDate').apply(
        lambda x: x.groupby('Days').apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[:1]]))
    max_expiration_days = df2.loc[df2['Days'].idxmax()]['Days']
    print(f'Max days option expiration is {max_expiration_days}')
    return df2


def filter_portfolio(portfolio_list: list, date: datetime.datetime):
    removed = []
    for l in portfolio_list:
        if l[1]['Expiration'] == date:
            removed.append(l)

    for r in removed:
        portfolio_list.remove(r)

    return removed


def calculate_delta_port(portfolio_list: list):
    delta_total = 0
    for p in portfolio_list:
        delta_total = delta_total + p[1]['Delta']

    return delta_total
