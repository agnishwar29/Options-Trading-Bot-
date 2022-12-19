import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
from thetadata import ThetaClient, OptionReqType, OptionRight, DateRange
import traceback


class AutomateTrade:

    def __init__(self, *, theta_profile, theta_pass):
        self.theta_profile = theta_profile
        self.theta_password = theta_pass

        self.client = self.theta_client()

    def theta_client(self):
        return ThetaClient(username=self.theta_profile, passwd=self.theta_password)

    def create_signals(self, ticker, exp_date):

        # dictionary to hold the transaction data
        transactions = {
            "transaction_date": [],
            "ticker": [],
            "strike": [],
            "exp_date": [],
            "transaction_type": []
        }

        strikes = self.client.get_strikes(ticker, exp=exp_date)

        self.fetch_call_signals(strikes, transactions, ticker, exp_date)
        self.fetch_put_signals(strikes, transactions, ticker, exp_date)

        pd.DataFrame(transactions).to_csv("transactions.csv")

    # fetching data and generating signals for CALL signals
    def fetch_call_signals(self, strikes, transactions, ticker, exp_date):

        # iterating over the strikes data
        for strike in strikes:
            # trying to get the data with that strike
            try:
                data = self.client.get_hist_option(
                    req=OptionReqType.EOD,
                    root=ticker,
                    exp=exp_date,
                    strike=strike,
                    right=OptionRight.CALL,
                    date_range=DateRange(exp_date - dt.timedelta(90), exp_date)
                )

                # checking if the length of the data is greater than 10
                if len(data) > 10:
                    # modifying the column names
                    data.columns = ["Open", "High", "Low", "Close", "Volume", "Count", "Date"]

                    # setting the index based on dates
                    data.set_index("Date", inplace=True)

                    # buying signal will depend on this condition
                    # if the data volume is greater than the volume data mean +
                    # 3x of standard deviation of the volume
                    data['Signal'] = data['Volume'] > data['Volume'].mean() + 3 * data['Volume'].std()

                    # selecting the data where it is having True value based on the previous condition
                    selected_data = data[data['Signal']]

                    # iterating over the selected rows and appending the data to the transaction dictionary
                    for index, row in selected_data.iterrows():
                        transactions['ticker'].append(ticker)
                        transactions['transaction_date'].append(index)
                        transactions['strike'].append(strike)
                        transactions['transaction_type'].append("BUY")
                        transactions['exp_date'].append(exp_date)

            except Exception as e:
                print(str(e))

    # fetching data and generating signals for PUT signals
    def fetch_put_signals(self, strikes, transactions, ticker, exp_date):

        # iterating over the strikes data
        for strike in strikes:
            # trying to get the data with that strike
            try:
                data = self.client.get_hist_option(
                    req=OptionReqType.EOD,
                    root=ticker,
                    exp=exp_date,
                    strike=strike,
                    right=OptionRight.PUT,
                    date_range=DateRange(exp_date - dt.timedelta(90), exp_date)
                )

                # checking if the length of the data is greater than 10
                if len(data) > 10:
                    # modifying the column names
                    data.columns = ["Open", "High", "Low", "Close", "Volume", "Count", "Date"]

                    # setting the index based on dates
                    data.set_index("Date", inplace=True)

                    # buying signal will depend on this condition
                    # if the data volume is greater than the volume data mean +
                    # 3x of standard deviation of the volume
                    data['Signal'] = data['Volume'] > data['Volume'].mean() + 3 * data['Volume'].std()

                    # selecting the data where it is having True value based on the previous condition
                    selected_data = data[data['Signal']]

                    # iterating over the selected rows and appending the data to the transaction dictionary
                    for index, row in selected_data.iterrows():
                        transactions['ticker'].append(ticker)
                        transactions['transaction_date'].append(index)
                        transactions['strike'].append(strike)
                        transactions['transaction_type'].append("SELL")
                        transactions['exp_date'].append(exp_date)

            except Exception as e:
                print(str(e))

    def backTest(self, ticker, exp_date):

        df = pd.read_csv("transactions.csv")

        # sorting the data by transaction dates
        df = df.sort_values(by="transaction_date")

        df.set_index("transaction_date", inplace=True)

        if "Unnamed: 0" in df.columns:
            df.drop(['Unnamed: 0'], axis=1, inplace=True)

        strikes = sorted(set(df.strike.values))

        total_profit = 0

        for strike in strikes:
            data = self.client.get_hist_option(
                req=OptionReqType.EOD,
                root=ticker,
                exp=exp_date,
                strike=strike,
                right=OptionRight.CALL,
                date_range=DateRange(exp_date - dt.timedelta(90), exp_date)
            )

            data.columns = ["Open", "High", "Low", "Close", "Volume", "Count", "Date"]

            data.set_index("Date", inplace=True)

            # BUY DATA
            plt.plot(data.index, data.Close)
            buy_data = df[df.transaction_type == "BUY"]
            buy_data = buy_data[buy_data.strike == strike]
            filtered_data = data[data.index.isin(buy_data.index)]
            plt.scatter(filtered_data.index, filtered_data.Close, marker="^", color="green")

            # SELL DATA
            plt.plot(data.index, data.Close)
            sell_data = df[df.transaction_type == "SELL"]
            sell_data = sell_data[sell_data.strike == strike]
            filtered_data = data[data.index.isin(sell_data.index)]
            plt.scatter(filtered_data.index, filtered_data.Close, marker="v", color="red")

            # plt.show()

            amount_owned = 0
            profit = 0

            for idx, row in df[df.strike == strike].iterrows():
                if row.transaction_type == "BUY":
                    amount_owned += 1
                    profit -= data[data.index == idx]['Close'].values[0]

                else:
                    if amount_owned > 0:
                        profit = data[data.index == idx]['Close'].values[0] * amount_owned
                        amount_owned = 0

            profit += amount_owned * data.iloc[-1]['Close']

            print(f"Profit: {round(profit, 2)}")

            total_profit += profit

        print(f"Total Profit: {round(total_profit, 2)}")

    # creating signals
    def connect(self, ticker):
        with self.client.connect():
            exp_dates = self.client.get_expirations(ticker)

            for exp_date in exp_dates[-50:]:
                print(f"Initiating with exp_date: {exp_date}")
                try:
                    self.create_signals(ticker, exp_date)
                    print(f"Created Signals: {exp_date}")

                    self.backTest(ticker, exp_date)
                    print(f"Back-tested: {exp_date}")
                except Exception as e:
                    print(str(e))


with open("pass.txt") as f:
    password = f.read()

trade = AutomateTrade(theta_profile="agnishwar39@gmail.com", theta_pass=password)
trade.connect("BMY")
