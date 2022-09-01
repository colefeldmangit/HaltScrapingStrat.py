import datetime
import feedparser
import pandas as pd
from ib_insync import Stock, util


def func_timer(func):
    def wrapper(*args, **kwargs):
        name = str(func.__name__)
        start = datetime.datetime.now()
        func(*args, **kwargs)
        end = datetime.datetime.now()
        print(name + ": " + str(end - start))
    return wrapper


class RSS:
    def __init__(self, ib_conn_, test_=False):
        self.halts_processed = []
        self.ib = ib_conn_
        self.halts_current = pd.DataFrame(columns=['reason', 'time', 'halt_price'], index = ['symbol'])
        self.test = test_
        self.isRoundDone = False

    @func_timer
    def fetch_halts(self):
        self.isRoundDone = False
        self.halts_current.dropna(inplace=True)
        if self.test:
            self.halts_current.loc['AAPL'] = ['LUDP',
                                         pd.to_datetime(datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")), None]
            self.halts_current.loc['MSFT'] = ['LUDP', pd.to_datetime((datetime.datetime.now() -
                                                                         datetime.timedelta(minutes=1)).strftime(
                "%m/%d/%Y %H:%M:%S")), None]
            self.halts_current.loc['GRRR'] = ['LUDP', pd.to_datetime((datetime.datetime.now() -
                                                                         datetime.timedelta(minutes=10)).strftime(
                "%m/%d/%Y %H:%M:%S")), None]
        else:
            # clear the current halts
            self.halts_current = self.halts_current.iloc[0:0]

            nasdaq_src = 'http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts'
            now = datetime.datetime.now()
            current_minute = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute).strftime(
                "%m/%d/%Y, %H:%M")
            print("Fetching Halts - {}".format(current_minute))

            items = feedparser.parse(nasdaq_src).entries
            # print("Raw Data")
            # print(items)

            for i in items:
                # only grab the LUDP halts that have not resumed
                if i["ndaq_reasoncode"] == "LUDP":  # and i["ndaq_resumptiontradetime"] == "":
                    data = {"symbol": i["title"],
                            "reason": i["ndaq_reasoncode"],
                            "timestamp": "{} {}".format(
                                i["ndaq_haltdate"],
                                i["ndaq_halttime"],
                            ),
                            "halt_price": None
                            }
                    timestamp = datetime.datetime.strptime(data["timestamp"], "%m/%d/%Y %H:%M:%S") - datetime.timedelta(hours = 1)
                    data.update({"timestamp": timestamp})
                    # halts.append(data)
                    if data not in self.halts_processed:
                        self.halts_processed.append(data)
                        self.halts_current.loc[len(self.halts_current)] = [data["symbol"], data["reason"],
                                                                           data["timestamp"], data['halt_price']]

    @func_timer
    def clean_halts_list(self):
        for i in self.halts_current.iterrows():
            # print(i[1]): attributes of halt
            symbol = i[0]
            time = i[1]['time']
            stock = Stock(symbol, 'SMART', 'USD')
            bars = self.ib.reqHistoricalData(
                stock, endDateTime='', durationStr='1 D',
                barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
            try:
                df = util.df(bars).set_index('date')
                # grabs volume for 5 mins leading up to halt time
                prev_min_volume = df['volume'].loc[
                                  time - datetime.timedelta(minutes=4):time].sum()
                #print(df)
                # if last 5 mins of volume has been less than n*100 shares
                if prev_min_volume < 100 or time < datetime.datetime.now() - datetime.timedelta(minutes=5):
                    self.remove_halt(symbol)
                else:
                    self.halts_current.at[str(symbol), 'halt_price'] = df['close'].iloc[-1]
            # TODO: add the except statement for the empty dataframe, record the halt price
            except AttributeError:
                self.remove_halt(symbol)


    def remove_halt(self, symbol):
        print("Removing symbol {}".format(symbol))
        self.halts_current.drop(index = symbol, axis = 0, inplace = True)

    def remove_all_halts(self):
        self.halts_current = self.halts_current.iloc[0:0]
        
    def setRoundDone(self):
        self.isRoundDone = True