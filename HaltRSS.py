import datetime
import feedparser
import pandas as pd


class RSS:
    def __init__(self, test_=False):
        self.halts_processed = []
        self.halts_current = pd.DataFrame(columns=['symbol', 'reason', 'time', 'halt_price'])
        self.test = test_

    def fetch_halts(self):
        if self.test:
            self.halts_current.loc[0] = ['AAPL', 'LUDP',
                                         pd.to_datetime(datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")), None]
            self.halts_current.loc[1] = ['GRRRX', 'LUDP', pd.to_datetime((datetime.datetime.now() -
                                                                          datetime.timedelta(minutes=1)).strftime(
                "%m/%d/%Y %H:%M:%S")), None]
            self.halts_current.loc[2] = ['MSFT', 'LUDP', pd.to_datetime((datetime.datetime.now() -
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
                    timestamp = datetime.datetime.strptime(data["timestamp"], "%m/%d/%Y %H:%M:%S")
                    data.update({"timestamp": timestamp})
                    # halts.append(data)
                    if data not in self.halts_processed:
                        self.halts_processed.append(data)
                        self.halts_current.loc[len(self.halts_current)] = [data["symbol"], data["reason"],
                                                                           data["timestamp"], data['halt_price']]

    def remove_halt(self, symbol):
        print("Removing symbol {}".format(symbol))
        self.halts_current = self.halts_current[self.halts_current['symbol'] != symbol]
        self.halts_current.reset_index(drop=True, inplace=True)
