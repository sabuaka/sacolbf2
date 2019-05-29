# -*- coding: utf-8 -*-
'''
    - collector module -
    broker: bitFlyer
    part: dataset child class for sfd
'''
from sautility.num import n2d


class DatasetSFD():
    '''class for dataset of tick'''

    def __init__(self):

        self.SFD_DECISION_TABLE = (
            (n2d(0.05), n2d(0.0025)),   # At least 5% but less than 10%	0.25% of the settlement
            (n2d(0.10), n2d(0.0050)),   # At least 10% but less than 15% 0.50% of the settlement
            (n2d(0.15), n2d(0.0100)),   # At least 15% but less than 20% 1.00% of the settlement
            (n2d(0.20), n2d(0.0200)),   # At least 20% 2.00% of the settlement
        )
        self.price_disparity_rate = None
        self.sfd_rate = None

        self.spot_lpt = None
        self.fx_lpt = None

    @property
    def price_disparity_per(self):
        '''price disparity (unit percent) '''
        if self.price_disparity_rate is None:
            return None
        return self.price_disparity_rate * n2d(100.0)

    @property
    def sfd_per(self):
        '''sfd (unit percent) '''
        if self.sfd_rate is None:
            return None
        return self.sfd_rate * n2d(100.0)

    def __update_date(self):
        if (self.spot_lpt is None or self.fx_lpt is None):
            self.price_disparity_rate = None
            self.sfd_rate = None
            return

        self.price_disparity_rate = (self.fx_lpt / self.spot_lpt) - n2d(1.0)
        self.sfd_rate = n2d(0.0)
        for row in reversed(self.SFD_DECISION_TABLE):
            if self.price_disparity_rate >= row[0]:
                self.sfd_rate = row[1]
                break

    def update_date_spot(self, spot_lpt):
        '''update data for spot'''
        self.spot_lpt = spot_lpt
        self.__update_date()

    def update_date_fx(self, fx_lpt):
        '''update data for fx'''
        self.fx_lpt = fx_lpt
        self.__update_date()
