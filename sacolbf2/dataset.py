# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: dataset parent class
'''
from saapibf import RealtimeAPI as RTAPI
from sautility.num import n2d

from .dsc_depth import DatasetDepth
from .dsc_trade import DatasetTrade
from .dsc_tick import DatasetTick
from .dsc_sfd import DatasetSFD

from .time_adjuster import TimeAdjuster


class SADataset():
    '''Dataset parent class'''

    DEFAULT_KEEP_TIME = 60  # sec

    def __init__(self):
        self.dsc_depth_fx = DatasetDepth()
        self.dsc_trade_fx = DatasetTrade(self.DEFAULT_KEEP_TIME)
        self.dsc_tick_fx = DatasetTick(self.DEFAULT_KEEP_TIME)
        self.dsc_sfd = DatasetSFD()

        self.__adjtime = TimeAdjuster.get_singleton()

    def get_now(self):
        '''
        Gets the now time that was adjusted by the NTP server.
        ---
        The collector module is time adjust for an issue with wrong time of VPS server.
        '''
        return self.__adjtime.get_now()

    def analyze_depth_ss(self, pair, data):
        '''analyze depth snapshot data'''
        if pair == RTAPI.TradePair.BTC_JPY.value:
            pass
        elif pair == RTAPI.TradePair.FX_BTC_JPY.value:
            self.dsc_depth_fx.init_data(data.mid_price, data.asks, data.bids, mpf=True)

    def analyze_depth_df(self, pair, data):
        '''analyze depth difference data'''
        if pair == RTAPI.TradePair.BTC_JPY.value:
            pass
        elif pair == RTAPI.TradePair.FX_BTC_JPY.value:
            self.dsc_depth_fx.update_data(data.mid_price, data.asks, data.bids, mpf=True)

    def analyze_trade(self, pair, data):
        '''analyze trade data'''
        if pair == RTAPI.TradePair.BTC_JPY.value:
            pass
        elif pair == RTAPI.TradePair.FX_BTC_JPY.value:
            self.dsc_trade_fx.update_date(data)

    def analyze_ticker(self, pair, data):
        '''analyze tick data'''
        if pair == RTAPI.TradePair.BTC_JPY.value:
            self.dsc_sfd.update_date_spot(n2d(data.ltp))
        elif pair == RTAPI.TradePair.FX_BTC_JPY.value:
            self.dsc_tick_fx.update_date(data)
            self.dsc_sfd.update_date_fx(n2d(data.ltp))
