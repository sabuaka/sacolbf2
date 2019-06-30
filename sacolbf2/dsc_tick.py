# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: dataset child class for tick
'''
from datetime import datetime, timedelta
from enum import IntEnum
from operator import itemgetter

from saapibf import RealtimeAPI as RTAPI
from sautility.num import n2d

from .time_adjuster import TimeAdjuster


class DatasetTick():
    '''class for dataset of tick'''

    BROKER_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

    class TRADE_PRICE_ARRAY(IntEnum):
        '''array position '''
        TIME = 0
        PRICE = 1

    def __init__(self, keep_time):

        self.__prm_keep_time = keep_time

        self.ts_dt = None
        self.bid_price = None
        self.ask_price = None
        self.bid_amount = None
        self.ask_amount = None
        self.total_bid_amount = None
        self.total_ask_amount = None
        self.trade_price = None
        self.trade_volume_24h = None
        self.spread = None
        self.spread_rate = None
        self.tick_data_list = []
        self.__keep_price_max = None
        self.__keep_price_min = None

        self.__available = False
        self.__adjtime = TimeAdjuster.get_singleton()

    @property
    def price_max(self):
        '''For backward compatible (<= 1.x.x)'''
        return self.__keep_price_max

    @property
    def price_min(self):
        '''For backward compatible (<= 1.x.x)'''
        return self.__keep_price_min

    @property
    def price_list(self):
        '''For backward compatible (<= 1.x.x)'''
        return self.tick_data_list

    def is_available(self):
        '''data available'''
        return self.__available

    def prmset_keep_time(self, seconds=0, milliseconds=0):
        '''set parameter of keep_time'''
        if seconds > 0:
            self.__prm_keep_time = seconds * 1000
        elif milliseconds > 0:
            self.__prm_keep_time = milliseconds

    class RTMC():
        '''real-time moving candlestick'''
        def __init__(self, tick_list: list):
            # p_: price r_: range c_: candle info
            self.p_open = None
            self.p_high = None
            self.p_low = None
            self.p_close = None
            self.p_body_high = None
            self.p_body_low = None
            self.r_hight = None
            self.r_body = None
            self.r_upper_shadow = None
            self.r_lower_shadow = None
            self.c_white = False
            self.c_black = False

            self.__analize_data(tick_list)

        def __analize_data(self, tick_list: list):
            if tick_list is None or len(tick_list) <= 0:
                return

            tick_list.sort(key=itemgetter(DatasetTick.TRADE_PRICE_ARRAY.TIME))
            __price_list = [row[DatasetTick.TRADE_PRICE_ARRAY.PRICE] for row in tick_list]

            self.p_open = n2d(__price_list[0])
            self.p_close = n2d(__price_list[-1])
            self.p_high = n2d(max(__price_list))
            self.p_low = n2d(min(__price_list))
            self.r_hight = n2d(self.p_high - self.p_low)

            if self.p_open < self.p_close:
                self.c_white = True
                self.c_black = False
                self.r_body = n2d(self.p_close - self.p_open)
                self.r_upper_shadow = n2d(self.p_high - self.p_close)
                self.r_lower_shadow = n2d(self.p_open - self.p_low)
                self.p_body_high = self.p_close
                self.p_body_low = self.p_open

            elif self.p_open > self.p_close:
                self.c_white = False
                self.c_black = True
                self.r_body = n2d(self.p_open - self.p_close)
                self.r_upper_shadow = n2d(self.p_high - self.p_open)
                self.r_lower_shadow = n2d(self.p_close - self.p_low)
                self.p_body_high = self.p_open
                self.p_body_low = self.p_close

            else:
                self.c_white = False
                self.c_black = False
                self.r_body = n2d(self.p_open - self.p_close)
                self.r_upper_shadow = n2d(self.p_high - self.p_open)
                self.r_lower_shadow = n2d(self.p_close - self.p_low)
                self.p_body_high = self.p_open
                self.p_body_low = self.p_close

    def get_rtmc(self, seconds=0, milliseconds=0):
        '''get real-time moving candlestick'''
        range_ms = seconds * 1000 if seconds > 0 else milliseconds
        range_dt = self.__adjtime.get_now() - timedelta(milliseconds=range_ms)
        range_list = [td for td in self.tick_data_list if td[self.TRADE_PRICE_ARRAY.TIME] > range_dt]
        if len(self.tick_data_list) <= len(range_list):
            range_list = None

        return self.RTMC(range_list)

    def __update_tick_data_list(self, dt, price):
        # add new data
        self.tick_data_list.append([dt, price])

        # remove rangeout data
        range_dt = self.__adjtime.get_now() - timedelta(milliseconds=self.__prm_keep_time)
        new_lst = [td for td in self.tick_data_list if td[self.TRADE_PRICE_ARRAY.TIME] > range_dt]
        self.tick_data_list.clear()
        self.tick_data_list.extend(new_lst)
        del new_lst

        # calculate maximum and minimum
        prices = [row[1] for row in self.tick_data_list]
        if prices is not None and len(prices) > 0:
            self.__keep_price_max = max(prices)
            self.__keep_price_min = min(prices)
        else:
            self.__keep_price_max = None
            self.__keep_price_min = None

    def update_date(self, data: RTAPI.TickerData):
        '''update data'''
        self.__available = False

        wk_utc_dt = datetime.strptime(data.timestamp[0:23], self.BROKER_TIMESTAMP_FORMAT)
        self.ts_dt = wk_utc_dt + timedelta(hours=9)
        self.bid_price = n2d(data.best_bid)
        self.ask_price = n2d(data.best_ask)
        self.bid_amount = n2d(data.best_bid_size)
        self.ask_amount = n2d(data.best_ask_size)
        self.total_bid_amount = n2d(data.total_bid_depth)
        self.total_ask_amount = n2d(data.total_ask_depth)
        self.trade_price = n2d(data.ltp)
        self.trade_volume_24h = n2d(data.volume_by_product)
        self.spread = self.ask_price - self.bid_price
        self.spread_rate = (self.ask_price / self.bid_price) - n2d(1.0)

        self.__update_tick_data_list(self.ts_dt, self.trade_price)

        self.__available = True
