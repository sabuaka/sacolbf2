# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: dataset child class for depth
'''
from decimal import Decimal
import numpy as np

from sautility.num import n2d


class DatasetDepth():
    '''class for dataset of depth'''

    def __init__(self, max_len=300):

        self.PRM_MAX_LEN = max_len

        self.mid_price = None
        self.asks = None
        self.bids = None

    @staticmethod
    def __mk_empty_depth_data():
        return np.empty((0, 2), dtype=np.dtype(Decimal))

    def __update_depth(self, raw_list, depth_list, diff, asc, mpf):
        # make new list by numpy
        new_list = depth_list
        for data in raw_list:
            # get each detail data
            new_price = n2d(data['price'])
            new_amount = n2d(data['size'])

            # delete old data (only diff mode)
            if diff:
                new_list = np.delete(new_list,
                                     np.where(depth_list[:, 0] == new_price),
                                     axis=0)
            # add new data
            if new_amount != 0:
                new_list = np.append(new_list, np.array([[new_price, new_amount]]), axis=0)

        # sort depth by price and adjust array length and mid price filter
        if asc:  # for ask
            new_list = new_list[np.argsort(new_list[:, 0])]

            if new_list.shape[0] > self.PRM_MAX_LEN:
                new_list = np.delete(new_list,
                                     np.where(new_list[:, 0] >= new_list[self.PRM_MAX_LEN - 1][0]),
                                     axis=0)

            if mpf:
                new_list = np.delete(new_list,
                                     np.where(new_list[:, 0] <= self.mid_price),
                                     axis=0)

        else:    # for bid
            new_list = new_list[np.argsort(new_list[:, 0])[::-1]]

            if new_list.shape[0] > self.PRM_MAX_LEN:
                new_list = np.delete(new_list,
                                     np.where(new_list[:, 0] <= new_list[self.PRM_MAX_LEN - 1][0]),
                                     axis=0)

            if mpf:
                new_list = np.delete(new_list,
                                     np.where(new_list[:, 0] >= self.mid_price),
                                     axis=0)

        # return new list
        return new_list

    def is_available(self):
        '''data available'''
        if (self.mid_price is not None
                and self.asks is not None
                and self.bids is not None):
            return True
        return False

    def init_data(self, raw_mid_price, raw_ask_list, raw_bid_list, mpf=True):
        '''initialize data (for snapshot data)'''
        # set mid price
        self.mid_price = n2d(raw_mid_price)

        # set ask depth
        if len(raw_ask_list) > 0:
            self.asks = self.__mk_empty_depth_data()
            self.asks = self.__update_depth(raw_ask_list, self.asks, diff=False, asc=True, mpf=mpf)

        # set bid depth
        if len(raw_bid_list) > 0:
            self.bids = self.__mk_empty_depth_data()
            self.bids = self.__update_depth(raw_bid_list, self.bids, diff=False, asc=False, mpf=mpf)

    def update_data(self, raw_mid_price, raw_ask_list, raw_bid_list, mpf=True):
        '''update data (for differential data)'''
        # initial check
        if self.asks is None or self.bids is None:
            return

        # set mid price
        self.mid_price = n2d(raw_mid_price)

        # set ask depth
        self.asks = self.__update_depth(raw_ask_list, self.asks, diff=True, asc=True, mpf=mpf)

        # set bid depth
        self.bids = self.__update_depth(raw_bid_list, self.bids, diff=True, asc=False, mpf=mpf)

    def get_range_depth(self, price_range):
        '''get range depth data'''
        # check data available
        if not self.is_available():
            return None, None

        range_ask_from = self.mid_price
        range_ask_to = self.mid_price + n2d(price_range)
        query_asks = self.asks[np.where(((self.asks[:, 0] >= range_ask_from)
                                         & (self.asks[:, 0] < range_ask_to)))]

        range_bid_from = self.mid_price
        range_bid_to = self.mid_price - price_range
        query_bids = self.bids[np.where(((self.bids[:, 0] <= range_bid_from)
                                         & (self.bids[:, 0] > range_bid_to)))]

        return query_asks, query_bids

    class SpreadInfo():
        '''spread information'''
        @staticmethod
        def __get_filter_top_idx(depth_data, filter_amount=None):
            if filter_amount is None:
                return 0
            idx = 0
            for idx in range(depth_data.shape[0]):
                if depth_data[idx][1] > filter_amount:
                    break
            if 0 < idx < depth_data.shape[0]:
                return idx
            return 0

        def __init__(self, depth_asks, depth_bids, amount_filter_ask, amount_filter_bid):
            self.amount_filter_ask = amount_filter_ask
            self.amount_filter_bid = amount_filter_bid

            self.idx_ask = self.__get_filter_top_idx(depth_asks, n2d(amount_filter_ask))
            self.idx_bid = self.__get_filter_top_idx(depth_bids, n2d(amount_filter_bid))
            self.price_ask = depth_asks[self.idx_ask][0]
            self.price_bid = depth_bids[self.idx_bid][0]
            self.spread = self.price_ask - self.price_bid
            self.spread_rate = (self.price_ask / self.price_bid) - n2d(1.0)

    def get_spread(self, amount_filter_ask=None, amount_filter_bid=None):
        '''get spread and spread(difference) rate'''
        return self.SpreadInfo(self.asks, self.bids, amount_filter_ask, amount_filter_bid)

    class StatisticsInfo():
        '''statistics information'''
        def __init__(self, depth_data):
            if len(depth_data[:, 1]) > 0:
                self.am_min = np.min(depth_data[:, 1], axis=0)
                self.am_max = np.max(depth_data[:, 1], axis=0)
                self.am_sum = np.sum(depth_data[:, 1], axis=0)
                self.am_mean = np.mean(depth_data[:, 1], axis=0)
                self.am_median = np.median(depth_data[:, 1], axis=0)
            else:
                self.am_min = n2d(0.0)
                self.am_max = n2d(0.0)
                self.am_sum = n2d(0.0)
                self.am_mean = n2d(0.0)
                self.am_median = n2d(0.0)

    def get_statistics(self, price_range):
        '''get statistics information'''
        if not self.is_available():
            return None, None

        r_asks, r_bids = self.get_range_depth(price_range)
        si_ask = self.StatisticsInfo(r_asks)
        si_bid = self.StatisticsInfo(r_bids)

        return si_ask, si_bid
