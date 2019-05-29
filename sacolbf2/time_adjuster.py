# -*- coding: utf-8 -*-'''
'''
    - time manager for collector module -
    This module is designed with the singleton pattern.
'''
from datetime import datetime, timedelta
from .ntplib import NTPClient

from .meta_singleton import Singleton


class TimeAdjuster():
    '''
    Class for time management
    -----
    Notes
    -----
    This is a singleton module. So the constructor can not use.
    Therefore, use get_singleton method to get the instance.
    '''

    def __new__(cls):
        raise NotImplementedError('Cannot initialize via constructor')

    @staticmethod
    def get_singleton():
        '''get singleton instance of singleton'''
        return Singleton_TimeAdjuster()


class Singleton_TimeAdjuster(metaclass=Singleton):
    '''
    This is singleton metaclass for TimeAdjuster class.
    Therefor, do not use from external modules.
    '''

    NTP_SERVER_HOST = 'ntp.nict.jp'

    def __init__(self):
        self.ntp_client = NTPClient()
        self.ntp_server_host = self.NTP_SERVER_HOST

        self.delta = timedelta()

    def update_delta(self):
        '''update delta between local and ntp'''
        try:
            res = self.ntp_client.request(self.ntp_server_host)
            ntp_now = datetime.fromtimestamp(res.tx_time)
            loc_now = datetime.now()
            self.delta = ntp_now - loc_now
        except:
            pass    # if failed do nothing.

    def get_now(self):
        '''get the adjusted now time'''
        return datetime.now() + self.delta

    def get_uts_s(self):
        '''get the unix timestamp in seconds (int type).'''
        return int(self.get_now().timestamp())
