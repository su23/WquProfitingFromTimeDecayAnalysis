import datetime
import time
import warnings
import os

import matplotlib.pyplot as plt
import pandas as pd
from pandas import DataFrame

warnings.filterwarnings('ignore')


class DataLoader:
    def __init__(self, asset_name):
        self.__asset_name = asset_name
        self.__max_depth = 4
        self.__df_call_days = [DataFrame] * self.__max_depth
        self.__df_put_days = [DataFrame] * self.__max_depth
        self.__tasks = []

    def get_call_puts(self, depth: int):
        if depth == 1:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows())
        if depth == 2:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows(),
                       self.__df_call_days[1].iterrows(), self.__df_put_days[1].iterrows())
        elif depth == 3:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows(),
                       self.__df_call_days[1].iterrows(), self.__df_put_days[1].iterrows(),
                       self.__df_call_days[2].iterrows(), self.__df_put_days[2].iterrows())
        elif depth == 4:
            return zip(self.__df_call_days[0].iterrows(), self.__df_put_days[0].iterrows(),
                       self.__df_call_days[1].iterrows(), self.__df_put_days[1].iterrows(),
                       self.__df_call_days[2].iterrows(), self.__df_put_days[2].iterrows(),
                       self.__df_call_days[3].iterrows(), self.__df_put_days[3].iterrows())
        else:
            raise NotImplementedError(f"Not implemented depth of {depth}")

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
        df_call = data.loc[data['Type'] == "call"]
        df_put = data.loc[data['Type'] == "put"]

        elapsed_time = time.process_time() - t
        print(f"End loading in {elapsed_time} seconds")
        # return df_call, df_put

        self.__select_closest_dn_option(df_call, self.__df_call_days, "call", -0.5)
        self.__select_closest_dn_option(df_put, self.__df_put_days, "put", 0.5)
        for i in range(1, self.__max_depth):
            self.__select_nth_call_by_delta_strike(df_call, self.__df_call_days, i + 1)
            self.__select_nth_put_by_delta_strike(df_put, self.__df_put_days, i + 1)

    def __select_closest_dn_option(self, df, target, option_type: str, delta: float):
        print("Start grouping")
        t = time.process_time()
        # df2 = df.apply(lambda x: x.groupby('Days').apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[begin:best]]))
            #.groupby('DataDate')
        df2 = df.groupby(['DataDate', 'Days']).apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[:1]])

        elapsed_time = time.process_time() - t
        print(f"Grouping processed in {elapsed_time} seconds")
        target[0] = df2
        df2.to_csv(f"grouped/{self.__asset_name}_{option_type}_0.csv")

    def __select_nth_call_by_delta_strike(self, df, target, best: int = 1):
        print("Start grouping")
        t = time.process_time()
        begin = best - 1
        # df2 = df.apply(lambda x: x.groupby('Days').apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[begin:best]]))
        # .groupby('DataDate')
        df2 = df[df.Delta < 0.5].groupby(['DataDate', 'Days'])
        df2 = df2.apply(lambda g: g.iloc[g['Delta'].argsort()[::-1][begin:best]])
        # ((g['Delta'] + 10.0) if g['Delta'] < delta else g['Delta'])

        # max_expiration_days = df2.loc[df2['Days'].idxmax()]['Days']
        # print(f'Max days option expiration is {max_expiration_days}')
        elapsed_time = time.process_time() - t
        print(f"Grouping processed in {elapsed_time} seconds")
        target[begin] = df2
        df2.to_csv(f"grouped/{self.__asset_name}_call_{begin}.csv")

    def __select_nth_put_by_delta_strike(self, df, target, best: int = 1):
        print("Start grouping")
        t = time.process_time()
        begin = best - 1
        # df2 = df.apply(lambda x: x.groupby('Days').apply(lambda g: g.iloc[(g['Delta'] + delta).abs().argsort()[begin:best]]))
        # .groupby('DataDate')
        df2 = df[df.Delta > -0.5].groupby(['DataDate', 'Days'])
        df2 = df2.apply(lambda g: g.iloc[(g['Delta']).argsort()[begin:best]])
           # df.filter(lambda x: x['Delta'] < delta)
        # ((g['Delta'] + 10.0) if g['Delta'] > delta else g['Delta'])

        # max_expiration_days = df2.loc[df2['Days'].idxmax()]['Days']
        # print(f'Max days option expiration is {max_expiration_days}')
        elapsed_time = time.process_time() - t
        print(f"Grouping processed in {elapsed_time} seconds")
        target[begin] = df2
        df2.to_csv(f"grouped/{self.__asset_name}_put_{begin}.csv")


class TradingStrategy:
    __min_expiration = 1
    __max_expiration = 15
    __max_delta = 1.5

    def __init__(self, loader: DataLoader, asset_name, best_options_count, strategy_name, initial_balance: float = 1000.0, terminate_if_bankrupt: bool = True):
        self.__loader = loader
        self.__asset_name = asset_name
        self.__strategy_name = strategy_name
        self.__best_options_count = best_options_count
        self.__balance = {0: initial_balance}
        self.__delta_tracing = {0: .0}
        self.__option_number = {0: 0}
        self.__portfolio_short_list = []
        self.__portfolio_long_list = []
        self.__i = 0
        self.__current_date = datetime.datetime.now()
        self.__terminate_if_bankrupt = terminate_if_bankrupt
        self.__became_bankrupt = False

    def getOptionState(self):
        return self.__option_number

    def simulate(self, max_days: int = 2000, trace: bool = False, show_plots: bool = False):
        t = time.process_time()

        call_puts = self.__loader.get_call_puts(self.__best_options_count)
        if self.__best_options_count == 1:
            self.simulate_internal(call_puts, max_days, trace)
        elif self.__best_options_count == 2:
            self.simulate_internal2(call_puts, max_days, trace)
        elif self.__best_options_count == 3:
            self.simulate_internal3(call_puts, max_days, trace)
        elif self.__best_options_count == 4:
            self.simulate_internal4(call_puts, max_days, trace)

        if show_plots:
            self.__visualize()

        elapsed_time = time.process_time() - t
        if trace:
            print(f"Elapsed time {elapsed_time} seconds")
        return self

    def status(self):
        if self.__became_bankrupt:
            print(f"{self.__asset_name} ({self.__strategy_name}): became bankrupt after {len(self.__balance)} days")
        else:
            print(f"{self.__asset_name} ({self.__strategy_name}): winning strategy. Final balance {list(self.__balance)[-1]}")

    def simulate_internal(self, call_puts, max_days, trace):
        for (_, call), (_, put) in call_puts:
            if self.__i > max_days:
                break
            if self.__balance[self.__i] < 0 and self.__terminate_if_bankrupt:
                self.__became_bankrupt = True
                break
            self.__trading_strategy([call], [put], trace)

    def simulate_internal2(self, call_puts, max_days, trace):
        for (_, call), (_, put), (_, call2), (_, put2) in call_puts:
            if self.__i > max_days:
                break
            if self.__balance[self.__i] < 0 and self.__terminate_if_bankrupt:
                self.__became_bankrupt = True
                break
            self.__trading_strategy([call, call2], [put, put2], trace)

    def simulate_internal3(self, call_puts, max_days, trace):
        for (_, call), (_, put), (_, call2), (_, put2), (_, call3), (_, put3) in call_puts:
            if self.__i > max_days:
                break
            if self.__balance[self.__i] < 0 and self.__terminate_if_bankrupt:
                self.__became_bankrupt = True
                break
            self.__trading_strategy([call, call2, call3], [put, put2, put3], trace)

    def simulate_internal4(self, call_puts, max_days, trace):
        for (_, call), (_, put), (_, call2), (_, put2), (_, call3), (_, put3), (_, call4), (_, put4) in call_puts:
            if self.__i > max_days:
                break
            if self.__balance[self.__i] < 0 and self.__terminate_if_bankrupt:
                self.__became_bankrupt = True
                break
            self.__trading_strategy([call, call2, call3, call4], [put, put2, put3, put4], trace)

    def __trading_strategy(self, calls: list, puts: list, trace: bool = False):
        assert len(calls) > 0
        assert len(puts) > 0

        date = calls[0]['DataDate']
        current_spot = calls[0]['UnderlyingPrice']

        self.__day_roll(date, current_spot, trace)

        days = calls[0]['Days']
        if days > self.__max_expiration or days < self.__min_expiration:
            return

        if not self._is_strategy_valid(calls, puts):
            #print(f"Is not valid")
            return

        total_delta = self._get_strategy_delta(calls, puts)
        #print(f"Total delta: {total_delta}")
        new_delta = self.__delta_tracing[self.__i] + total_delta
        if self.__max_delta < abs(new_delta):
            # print(f"{new_delta} is far from {self.__max_delta}. Current delta is {self.__delta_tracing[self.__i]}")
            if abs(new_delta) > abs(self.__delta_tracing[self.__i]):
                return

        self.__delta_tracing[self.__i] = new_delta
        self.__option_number[self.__i] += self._get_option_count()

        total_bid = .0
        for option in self._short_portfolio(calls, puts):
            bid = option['Bid']
            total_bid += bid
            self.__balance[self.__i] += bid
            self.__portfolio_short_list.append(option)

        if trace:
            print(f"Total income: {total_bid}, Total: {self.__balance[self.__i]}")

        total_ask = .0
        for option in self._long_portfolio(calls, puts):
            ask = option['Ask']
            total_ask += ask
            self.__balance[self.__i] -= ask
            self.__portfolio_long_list.append(option)
        if trace:
            print(f"Total pay: {total_ask}, Total: {self.__balance[self.__i]}")

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
        self.__i += 1

        self.__balance[self.__i] = self.__balance[self.__i - 1]
        self.__delta_tracing[self.__i] = self.__delta_tracing[self.__i - 1]
        self.__option_number[self.__i] = self.__option_number[self.__i - 1]

        # short portfolio
        expired_today_short = self.__get_expired_options(self.__portfolio_short_list, date)
        self.__portfolio_short_list = self.__remove_expired_options(self.__portfolio_short_list, date)

        self.__delta_tracing[self.__i] -= self.__calculate_delta_port(expired_today_short)
        self.__option_number[self.__i] -= len(expired_today_short)
        self.__balance[self.__i] += self.__should_execute_short(spot, expired_today_short, trace)

        # long portfolio
        expired_today_long = self.__get_expired_options(self.__portfolio_long_list, date)
        self.__portfolio_long_list = self.__remove_expired_options(self.__portfolio_long_list, date)

        self.__delta_tracing[self.__i] += self.__calculate_delta_port(expired_today_long)
        self.__option_number[self.__i] -= len(expired_today_long)
        self.__balance[self.__i] += self.__should_execute_long(spot, expired_today_long, trace)

    def __remove_expired_options(self, portfolio_list: list, date: datetime.datetime):
        return list(filter(lambda x: x['Expiration'] != date, portfolio_list))

    def __get_expired_options(self, portfolio_list: list, date: datetime.datetime):
        return list(filter(lambda x: x['Expiration'] == date, portfolio_list))

    def __calculate_delta_port(self, portfolio_list: list):
        delta_total = 0
        for p in portfolio_list:
            delta_total += p['Delta']

        return delta_total

    def __should_execute_short(self, spot, expired_today, trace):
        total_minus = 0
        executed = 0
        for option in expired_today:
            strike = option['Strike']
            option_type = option['Type']
            if option_type == 'call' and strike < spot:
                minus = strike - spot
                executed += 1
                total_minus += minus
            elif option_type == 'put' and strike > spot:
                minus = spot - strike
                executed += 1
                total_minus += minus

        total_len = len(expired_today)
        if total_len > 0 and trace:
            print(f"From {total_len} expired SHORT options only {executed} were executed. Total minus {total_minus}")

        return total_minus

    def __should_execute_long(self, spot, expired_today, trace):
        total_plus = 0
        executed = 0
        for option in expired_today:
            strike = option['Strike']
            option_type = option['Type']
            if option_type == 'call' and strike < spot:
                plus = spot - strike
                executed = executed + 1
                total_plus += plus
            elif option_type == 'put' and strike > spot:
                plus = strike - spot
                executed = executed + 1
                total_plus += plus

        total_len = len(expired_today)
        if total_len > 0 and trace:
            print(f"From {total_len} expired LONG options only {executed} were executed. Total plus {total_plus}")

        return total_plus

    def __visualize(self):
        suffix = f"{self.__asset_name} ({self.__strategy_name})"
        self.__visualize2(self.__balance, f"Daily balance {suffix})")
        self.__visualize2(self.__delta_tracing, f"Daily delta {suffix})")
        self.__visualize2(self.__option_number, f"Daily portfolio option count {suffix})")

    def __visualize2(self, b, caption):
        lists = sorted(b.items())
        x, y = zip(*lists)
        plt.title(caption)
        plt.plot(x, y)
        plt.show()
