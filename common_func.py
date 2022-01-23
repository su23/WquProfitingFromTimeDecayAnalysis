import datetime
import time
import warnings
import os

import matplotlib.pyplot as plt
import pandas as pd
from pandas import DataFrame

warnings.filterwarnings('ignore')


class DataLoader:
    __max_depth = 3
    __df_call_days = [DataFrame] * __max_depth
    __df_put_days = [DataFrame] * __max_depth
    __tasks = []

    def __init__(self, asset_name):
        self.__asset_name = asset_name

    def get_call_puts(self, depth: int):
        if depth == 2:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows(), self.__df_call_days[1].iterrows(), self.__df_put_days[1].iterrows())
        elif depth == 3:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows(), self.__df_call_days[1].iterrows(), self.__df_put_days[1].iterrows(), self.__df_call_days[2].iterrows(), self.__df_put_days[2].iterrows())
        else:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows())

    def load(self):
        asset = self.__asset_name
        all_is_ready = True
        for i in range(0, self.__max_depth):
            for option_type in ["call", "put"]:
                if not os.path.isfile(f"grouped/{asset}_{option_type}_{i}.csv"):
                    all_is_ready = False
                    break

        if all_is_ready:
            t = time.process_time()
            print(f"All files for {asset} are ready. Loading")
            for i in range(0, self.__max_depth):
                self.__df_call_days[i] = pd.read_csv(f'grouped/{asset}_call_{i}.csv')
                self.__df_put_days[i] = pd.read_csv(f'grouped/{asset}_put_{i}.csv')
            elapsed_time = time.process_time() - t
            print(f"End loading in {elapsed_time} seconds")
            return

        print("Start loading")
        t = time.process_time()
        data = pd.read_csv(f'{asset}.csv', sep=",").filter(['UnderlyingPrice', 'Type', 'Expiration',
                                                            'DataDate', 'Strike', 'Last', 'Bid', 'Ask', 'Volume',
                                                            'OpenInterest', 'IV', 'Delta', 'Gamma',
                                                            'Theta', 'Vega'])
        data['Expiration'] = pd.to_datetime(data['Expiration'])
        data['DataDate'] = pd.to_datetime(data['DataDate'])
        data['Days'] = (data['Expiration'] - data['DataDate']).astype('timedelta64[D]').astype(int)
        df_call = data.loc[data['Type'] == "call"].groupby(['DataDate', 'Days'])
        df_put = data.loc[data['Type'] == "put"].groupby(['DataDate', 'Days'])

        elapsed_time = time.process_time() - t
        print(f"End loading in {elapsed_time} seconds")
        # return df_call, df_put

        for i in range(0, self.__max_depth):
            self.__select_closest_dn_option_max_expiration_days(df_call, self.__df_call_days, "call", -0.5, i + 1)
            self.__select_closest_dn_option_max_expiration_days(df_put, self.__df_put_days, "put", 0.5, i + 1)

    def __select_closest_dn_option_max_expiration_days(self, df, target, option_type: str, delta: float = -0.5, best: int = 1):
        print("Start grouping")
        t = time.process_time()
        begin = best - 1
        # df2 = df.apply(lambda x: x.groupby('Days').apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[begin:best]]))
            #.groupby('DataDate')
        df2 = df.apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[begin:best]])

        # max_expiration_days = df2.loc[df2['Days'].idxmax()]['Days']
        # print(f'Max days option expiration is {max_expiration_days}')
        elapsed_time = time.process_time() - t
        print(f"Grouping processed in {elapsed_time} seconds")
        # print(df2)
        target[begin] = df2
        df2.to_csv(f"grouped/{self.__asset_name}_{option_type}_{begin}.csv")


class TradingStrategy:
    __loader: DataLoader = None
    __best_options_count: int = None
    __asset_name: str = None
    __min_expiration = 1
    __max_expiration = 15
    __balance = {0: .0}
    __delta_tracing = {0: .0}
    __option_number = {0: 0}

    __portfolio = []
    __max_delta = 1.5

    __i = 0
    __current_date = datetime.datetime.now()

    def __init__(self, loader: DataLoader, asset_name, best_options_count):
        self.__loader = loader
        self.__asset_name = asset_name
        self.__best_options_count = best_options_count

    def simulate(self):
        t = time.process_time()

        call_puts = self.__loader.get_call_puts(self.__best_options_count)
        if self.__best_options_count == 1:
            self.simulate_internal(call_puts, max_days=100, trace=False)
        elif self.__best_options_count == 2:
            self.simulate_internal2(call_puts, max_days=100, trace=False)
        elif self.__best_options_count == 3:
            self.simulate_internal3(call_puts, max_days=100, trace=False)

        self.__visualize()
        elapsed_time = time.process_time() - t
        print(f"Elapsed time {elapsed_time} seconds")

    def simulate_internal(self, call_puts, max_days, trace):
        for (_, call), (_, put) in call_puts:
            self.__trading_strategy([call], [put], max_days, trace)

    def simulate_internal2(self, call_puts, max_days, trace):
        for (_, call), (_, put), (_, call2), (_, put2) in call_puts:
            self.__trading_strategy([call, call2], [put, put2], max_days, trace)

    def simulate_internal3(self, call_puts, max_days, trace):
        for (_, call), (_, put), (_, call2), (_, put2), (_, call3), (_, put3) in call_puts:
            self.__trading_strategy([call, call2, put3], [put, put2, put3], max_days, trace)

    def __trading_strategy(self, calls: list, puts: list, max_days: int = 100, trace: bool = False):
        assert len(calls) > 0
        assert len(puts) > 0
        if self.__i > max_days:
            return

        date = calls[0]['DataDate']
        current_spot = calls[0]['UnderlyingPrice']

        self.__day_roll(date, current_spot, trace)

        days = calls[0]['Days']
        if days > self.__max_expiration or days < self.__min_expiration:
            return

        total_delta = self._get_strategy_delta(calls, puts)

        new_delta = self.__delta_tracing[self.__i] + total_delta
        if self.__max_delta < new_delta:
            return

        if not self._is_strategy_valid(calls, puts):
            return

        self.__delta_tracing[self.__i] = new_delta
        self.__option_number[self.__i] = self.__option_number[self.__i] + 2

        total_bid = .0
        for option in self._short_portfolio(calls, puts):
            bid = option['Bid']
            total_bid += bid
            self.__balance[self.__i] += bid
            self.__portfolio.append(option)

        if trace:
            print(f"Total income: {total_bid}, Total: {self.__balance[self.__i]}")

        # todo: same for long

    def _get_option_count(self) -> int:
        raise NotImplementedError("Please Implement this method")

    def _get_strategy_delta(self, calls: list, puts: list) -> float:
        raise NotImplementedError("Please Implement this method")

    def _is_strategy_valid(self, calls: list, puts: list) -> bool:
        raise NotImplementedError("Please Implement this method")

    def _short_portfolio(self, calls: list, puts: list) -> list:
        raise NotImplementedError("Please Implement this method")

    def _long_portfolio(self, calls: list, puts: list) -> list:
        raise NotImplementedError("Please Implement this method")

    def __day_roll(self, date, spot, trace):
        if date == self.__current_date:
            return

        self.__current_date = date
        if trace:
            print(f'Next date {date}')
        self.__i = self.__i + 1

        self.__balance[self.__i] = self.__balance[self.__i - 1]
        self.__delta_tracing[self.__i] = self.__delta_tracing[self.__i - 1]
        self.__option_number[self.__i] = self.__option_number[self.__i - 1]

        expired_today = self.__get_expired_options(self.__portfolio, date)
        self.__portfolio = self.__remove_expired_options(self.__portfolio, date)

        self.__delta_tracing[self.__i] = self.__delta_tracing[self.__i] - self.__calculate_delta_port(expired_today)
        self.__option_number[self.__i] = self.__option_number[self.__i] - len(expired_today)
        self.__balance[self.__i] = self.__balance[self.__i] + self.__should_execute(spot, expired_today, trace)

    def __remove_expired_options(self, portfolio_list: list, date: datetime.datetime):
        return list(filter(lambda x: x['Expiration'] != date, portfolio_list))

    def __get_expired_options(self, portfolio_list: list, date: datetime.datetime):
        return list(filter(lambda x: x['Expiration'] == date, portfolio_list))

    def __calculate_delta_port(self, portfolio_list: list):
        delta_total = 0
        for p in portfolio_list:
            delta_total = delta_total + p['Delta']

        return delta_total

    def __should_execute(self, spot, expired_today, trace):
        total_minus = 0
        executed = 0
        for option in expired_today:
            strike = option['Strike']
            option_type = option['Type']
            if option_type == 'call' and strike < spot:
                minus = strike - spot
                executed = executed + 1
                total_minus = total_minus + minus
            elif option_type == 'put' and strike > spot:
                minus = spot - strike
                executed = executed + 1
                total_minus = total_minus + minus

        total_len = len(expired_today)
        if total_len > 0 and trace:
            print(f"From {total_len} expired options only {executed} were executed. Total minus {total_minus}")

        return total_minus

    def __visualize(self):
        self.__visualize2(self.__balance, "Daily balance")
        self.__visualize2(self.__delta_tracing, "Daily delta")
        self.__visualize2(self.__option_number, "Daily portfolio option count")

    def __visualize2(self, b, caption):
        lists = sorted(b.items())
        x, y = zip(*lists)
        plt.title(caption)
        plt.plot(x, y)
        plt.show()
