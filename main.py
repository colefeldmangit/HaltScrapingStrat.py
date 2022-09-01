import datetime
from collections import deque
from ib_insync import *
import asyncio
import pandas as pd
import numpy as np
import time
from HaltRSS import RSS

class NoHaltsLeftError(Exception):
    """ Thrown when all halts in the RSS are taken care of"""
    pass


ib = IB()
rss = RSS(ib, True)
ib.connect('127.0.0.1', 7496, clientId=1)
market_data_df = pd.DataFrame(columns = ['time','last', 'halted'], index = ['symbol'])
print(market_data_df)

def onPendingTicker(tickers):
    for t in tickers:
        #market_data_df.loc[t.contract.symbol] = [t.time, t.last, t.halted]
        if t.last != rss.halts_current.loc[str(t.contract.symbol)]['halt_price']:
            print(str(t.contract.symbol) + " unhalted")
            #TODO: write strategy inside here I guess
    if rss.isRoundDone:
        raise NoHaltsLeftError
    rss.setRoundDone()


if __name__ == "__main__":
    # updates the halt list and adds to the processed halts
    rss.fetch_halts()
    print(rss.halts_current)
    rss.clean_halts_list()
    print(rss.halts_current)
    test1 = True
    if rss.halts_current.empty:
        time.sleep(60)
    else:
        stocks = [Stock(i, 'SMART', 'USD') for i in rss.halts_current.index]
        for stock in stocks:
            ib.reqMktData(stock, '', False, False)
        ib.pendingTickersEvent += onPendingTicker
    try:
        ib.run()
    except NoHaltsLeftError:
        ib.disconnect()


        #ib.cancelMktData(stocks[0])


    #TODO: with halts remaining request the market data

    # Halt Logic:
    ''' Have halt dataframe containing all the of the last halt prices. onPendingTickers method called on every update,
    add logic to compare each update to the halt price. On a change check for a gap up, place bracket order, start minute timer'''



