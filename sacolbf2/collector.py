# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: main class
'''
from enum import IntEnum, auto

from sautility import dt
from saapibf import RealtimeAPI

from .dataset import SADataset
from .time_adjuster import TimeAdjuster


class SACollector():
    '''Collector class'''

    __API_PING_INTERVAL = 30    # sec
    __API_PING_TIMEOUT = 10     # sec

    __LISTEN_CHANNELS = [
        RealtimeAPI.ListenChannel.BOARD_SNAPSHOT_FX_BTC_JPY,
        RealtimeAPI.ListenChannel.BOARD_FX_BTC_JPY,
        RealtimeAPI.ListenChannel.EXECUTIONS_FX_BTC_JPY,
        RealtimeAPI.ListenChannel.TICKER_FX_BTC_JPY,
        RealtimeAPI.ListenChannel.TICKER_BTC_JPY
    ]

    class UpdateEvent(IntEnum):
        '''Change event type'''
        DEPTH = auto()
        TICK = auto()
        TRADE = auto()
        ERROR = auto()
        KEY_INTERRUPT_STOP = auto()

    def __init__(self, event_callback=None):

        self.__event_callback = event_callback

        self.__rt_api = self.__create_rtapi()

        self.dataset = SADataset()
        self.__adjtime = TimeAdjuster.get_singleton()
        self.__adjtime.update_delta()
        self.__updatetimer_adjtime = -1

    def __create_rtapi(self):
        '''create new realtime api instance.'''
        return RealtimeAPI(self.__LISTEN_CHANNELS,
                           on_message_board=self.__on_message_board,
                           on_message_board_snapshot=self.__on_message_board_snapshot,
                           on_message_ticker=self.__on_message_ticker,
                           on_message_executions=self.__on_message_executions,
                           on_error=self.__on_error,
                           ping_interval=self.__API_PING_INTERVAL,
                           ping_timeout=self.__API_PING_TIMEOUT)

    def __exec_adjust_time(self):
        if self.__updatetimer_adjtime != dt.get_minute():
            self.__adjtime.update_delta()
            self.__updatetimer_adjtime = dt.get_minute()

    def __exec_event_callback(self, event: UpdateEvent):
        if self.__event_callback:
            self.__event_callback(event, self.dataset)

    def __on_message_board(self, _, pair, data):
        self.__exec_adjust_time()
        self.dataset.analyze_depth_df(pair, data)
        self.__exec_event_callback(self.UpdateEvent.DEPTH)

    def __on_message_board_snapshot(self, _, pair, data):
        self.__exec_adjust_time()
        self.dataset.analyze_depth_ss(pair, data)
        self.__exec_event_callback(self.UpdateEvent.DEPTH)

    def __on_message_ticker(self, _, pair, data):
        self.__exec_adjust_time()
        self.dataset.analyze_ticker(pair, data)
        self.__exec_event_callback(self.UpdateEvent.TICK)

    def __on_message_executions(self, _, pair, datas):
        self.__exec_adjust_time()
        self.dataset.analyze_trade(pair, datas)
        self.__exec_event_callback(self.UpdateEvent.TRADE)

    def __on_error(self, _, ex):
        if self.__event_callback:
            if isinstance(ex, KeyboardInterrupt):
                self.__event_callback(self.UpdateEvent.KEY_INTERRUPT_STOP, None)
            else:
                self.__event_callback(self.UpdateEvent.ERROR, None)

    def start(self):
        '''Listen start'''
        self.__rt_api.start()

    def stop(self):
        '''Listen stop'''
        self.__rt_api.stop()
