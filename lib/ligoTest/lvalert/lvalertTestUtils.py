description = """utilities for sending lvalert messages as part of the FakeDb infrastructure"""
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import time

#-------------------------------------------------

class LVAlertBuffer():
    '''
    a class that wraps around a lvalert.out file produced by FakeDb
    provides some basic querying and manipulations for scripts that send lvalert messages
    '''

    def __init__(self, filename):
        if not os.path.exists(filename):
            raise ValueError('could not find filename=%s'%filename)
        self.filename  = filename
        self.setTimestamp()

    def getTimestamp(self):
        '''
        queries the timestamp associated with this file
        '''
        return os.path.getmtime(self.filename)

    def setTimestamp(self):
        '''
        updates the timestamp for this file
        '''
        self.timestamp = self.getTimestamp()

    def wasTouched(self):
        '''
        determines whether the file has been modified
        '''
        return self.timestamp!=self.getTimestamp()

    def extract(self):
        '''
        extracts the new messages and returns them

        NOT IMPLEMENTED
        '''
        raise NotImplementedError 

    def monitor(self, foo, cadence=0.1, *args, **kwargs):
        '''
        monitors the file, and when a change is detected we extract the call foo with signature:
        for node, message in self.extract():
            foo( node, message, *args, **kwargs )
        '''
        while True:
            t = time.time()
            if self.wasTouched():
                self.setTimestamp() ### update
                for node, message in self.extract():
                    foo( node, message, *args, **kwargs )
            wait = cadence - (time.time()-t)
            if wait:
                time.sleep(wait)
