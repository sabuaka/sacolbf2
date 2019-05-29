# -*- coding: utf-8 -*-
'''
The module for Singleton of GoF design pattern.
'''


class Singleton(type):
    '''
    Meta class for Singleton pattern.
    '''

    _instances = {}

    def __call__(cls, *args, **kwargs):
        '''
        Args:
            cls:        class object
            args:       class argument list
            kwargs:     class attribute tuple

        Returns:
            singleton instance
        '''

        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]
