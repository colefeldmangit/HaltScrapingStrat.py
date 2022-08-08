import datetime
from collections import deque
from ib_insync import *
import asyncio
import pandas as pd
import numpy as np
import time
from HaltRSS import RSS

ib = IB()
rss = RSS(True)

def onError(reqId, errorCode, errorString, contract):
    if errorCode == 200:
        rss.remove_halt(contract.symbol)



ib.errorEvent += onError
ib.connect('127.0.0.1', 7496, clientId=1)


if __name__ == "__main__":
    # updates the halt list and adds to the processed halts
    rss.fetch_halts()
    for i in rss.halts_current.iterrows():
        # print(i[1]): attributes of halt
        symbol = i[1]['symbol']
        time = i[1]['time']
        stock = Stock(symbol, 'SMART', 'USD')
        bars = ib.reqHistoricalData(
            stock, endDateTime='', durationStr='1 D',
            barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
        try:
            df = util.df(bars).set_index('date')
            # grabs volume for 5 mins leading up to halt time
            prev_min_volume = df['volume'].loc[
                              time - datetime.timedelta(minutes=4):time + datetime.timedelta(minutes=1)].sum()
            # if last 5 mins of volume has been less than n*100 shares
            if prev_min_volume < 100 or time < datetime.datetime.now()-datetime.timedelta(minutes = 5):
                rss.remove_halt(symbol)
        #TODO: add the except statement for the empty dataframe, record the halt price
        except AttributeError:
            rss.remove_halt(symbol)
    #TODO: with halts remaining request the market data

    #Halt Logic:
    ''' Have halt dataframe containing all the of the last halt prices. onPendingTickers method called on every update,
    add logic to compare each update to the halt price. On a change check for a gap up, place bracket order, start minute timer'''



