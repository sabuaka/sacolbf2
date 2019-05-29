# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: dataset child class for trade
'''
from datetime import datetime, timedelta
from decimal import Decimal
from enum import IntEnum

from sautility.num import n2d, dfloor

from .time_adjuster import TimeAdjuster


class DatasetTrade():
    '''
    dataset for trade data

    Parameters
    ----------
    keep_time : int
        data keeping time (sec)
    '''

    BROKER_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

    class TRADE_ARRAY(IntEnum):
        '''array position '''
        TIME = 0
        PRICE = 1
        AMOUNT = 2
        BUY_ID = 3
        SELL_ID = 4

    def __init__(self, keep_time):

        self.__prm_keep_time = keep_time
        self.buys = []
        self.sells = []
        self.last_price = None
        self.last_amount = None
        self.last_dt = None

        self.event_values = []    # 0:Time 1:price 2:amount

        self.__range_start_dt = None
        self.__adjtime = TimeAdjuster.get_singleton()

    def prmset_keep_time(self, seconds=0, milliseconds=0):
        '''set parameter of keep time'''
        if seconds > 0:
            self.__prm_keep_time = seconds * 1000
        elif milliseconds > 0:
            self.__prm_keep_time = milliseconds

    def is_available(self):
        '''data available'''
        if self.last_price is not None:
            return True
        return False

    def __add_data(self, exec_data):

        wk_utc_dt = datetime.strptime(exec_data.exec_date[0:22], self.BROKER_TIMESTAMP_FORMAT)
        val_dt = wk_utc_dt + timedelta(hours=9)
        val_price = n2d(exec_data.price)
        val_amount = n2d(exec_data.size)
        val_buy_id = exec_data.buy_child_order_acceptance_id
        val_sell_id = exec_data.sell_child_order_acceptance_id

        if exec_data.side == "BUY":
            self.buys.append([val_dt, val_price, val_amount, val_buy_id, val_sell_id])
        elif exec_data.side == "SELL":
            self.sells.append([val_dt, val_price, val_amount, val_buy_id, val_sell_id])

        self.event_values.append([val_dt, val_price, val_amount])

    def __remove_rangeout_data(self):

        range_dt = self.__adjtime.get_now() - timedelta(milliseconds=self.__prm_keep_time)

        def _create_new_list(target_list):
            new_lst = [ed for ed in target_list if ed[self.TRADE_ARRAY.TIME] > range_dt]
            target_list.clear()
            target_list.extend(new_lst)
            del new_lst

        _create_new_list(self.buys)
        _create_new_list(self.sells)

    def get_amount(self, seconds=None, milliseconds=None):
        '''get trade amount -> buy, sell'''

        range_ms = self.__prm_keep_time
        if seconds is not None and seconds > 0:
            range_ms = seconds * 1000
        elif milliseconds is not None and milliseconds > 0:
            range_ms = milliseconds

        def _query_data(target_list, prm_range):

            if len(target_list) > 0:
                sum_amount = n2d(0.0)
                query_list = [td for td in target_list if td[self.TRADE_ARRAY.TIME] >= prm_range]
                for exec_data in query_list:
                    sum_amount += exec_data[self.TRADE_ARRAY.AMOUNT]
                return sum_amount

            return n2d(0.0)

        prm_range = self.__adjtime.get_now() - timedelta(milliseconds=range_ms)

        amount_buy = _query_data(self.buys, prm_range)
        amount_sell = _query_data(self.sells, prm_range)

        return amount_buy, amount_sell

    def check_exec_buy(self, oid) -> (Decimal, Decimal):
        '''check the execution of buy order'''
        price_list = []
        amount_list = []
        total_price = n2d(0.0)
        total_amount = n2d(0.0)

        for datas in self.buys:  # taker
            if datas[self.TRADE_ARRAY.BUY_ID] == oid:
                price_list.append(datas[self.TRADE_ARRAY.PRICE])
                amount_list.append(datas[self.TRADE_ARRAY.AMOUNT])

        for datas in self.sells:  # maker
            if datas[self.TRADE_ARRAY.BUY_ID] == oid:
                price_list.append(datas[self.TRADE_ARRAY.PRICE])
                amount_list.append(datas[self.TRADE_ARRAY.AMOUNT])

        total_amount = sum(amount_list)
        for part_price, part_amount in zip(price_list, amount_list):
            total_price += (part_price * (part_amount / total_amount))

        return dfloor(total_price, 0), total_amount

    def check_exec_sell(self, oid) -> (Decimal, Decimal):
        '''check the execution of sell order'''
        # for exec_data in self.sells:
        price_list = []
        amount_list = []
        total_price = n2d(0.0)
        total_amount = n2d(0.0)

        for datas in self.buys:  # maker
            if datas[self.TRADE_ARRAY.SELL_ID] == oid:
                price_list.append(datas[self.TRADE_ARRAY.PRICE])
                amount_list.append(datas[self.TRADE_ARRAY.AMOUNT])

        for datas in self.sells:  # taker
            if datas[self.TRADE_ARRAY.SELL_ID] == oid:
                price_list.append(datas[self.TRADE_ARRAY.PRICE])
                amount_list.append(datas[self.TRADE_ARRAY.AMOUNT])

        total_amount = sum(amount_list)
        for part_price, part_amount in zip(price_list, amount_list):
            total_price += (part_price * (part_amount / total_amount))

        return dfloor(total_price, 0), total_amount

    def update_date(self, raw_executions_list):
        '''update data'''
        # init value
        self.last_amount = n2d(0.0)

        # check start time
        if self.__range_start_dt is None:
            self.__range_start_dt = self.__adjtime.get_now()

        # add new data
        self.event_values = []
        for exec_data in raw_executions_list:
            self.__add_data(exec_data)
            self.last_amount += n2d(exec_data.size)

        # remove out of range data
        self.__remove_rangeout_data()

        # get the last tread info
        wk_utc = datetime.strptime(raw_executions_list[-1].exec_date[0:23], self.BROKER_TIMESTAMP_FORMAT)
        self.last_dt = wk_utc + timedelta(hours=9)
        self.last_price = n2d(raw_executions_list[-1].price)
