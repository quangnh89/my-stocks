import argparse
import backtrader as bt
import pandas as pd
from stock.stock import Stock
from strategies.teststrategy import TestStrategy


def runstrat():
    args = parse_args()
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(0.001)
    # Add a strategy

    cerebro.addstrategy(TestStrategy)
    s = Stock('FPT', length=365)
    dataframe = s.df
    dataframe['tt'] = pd.to_datetime(dataframe['t'], unit='s')
    print('data', dataframe)
    data = bt.feeds.PandasData(dataname=dataframe, datetime='tt', open='o', high='h', low='l', close='c', volume='v',
                               openinterest='c')
    cerebro.adddata(data)

    print('Start Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Plot result
    # cerebro.plot(style='bar')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Pandas test script')

    parser.add_argument('--noheaders', action='store_true', default=False,
                        required=False,
                        help='Do not use header rows')

    parser.add_argument('--noprint', action='store_true', default=False,
                        help='Print the dataframe')

    return parser.parse_args()


if __name__ == '__main__':
    runstrat()
