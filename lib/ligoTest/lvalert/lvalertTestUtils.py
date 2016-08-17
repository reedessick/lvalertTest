description = """utilities for sending lvalert messages as part of the FakeDb infrastructure"""
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import time

#-------------------------------------------------

def alert2line( node, message ):
    '''
    prints an alert to a file in a standardized way
    specifically designed to be read out by a FileMonitor
    '''
    return "%s|%s"%(node, message)

def line2alert( line ):
    '''
    given a line from a file, does the inverse of alert2line
    '''
    return line.strip().split("|")

#-------------------------------------------------

class LVAlertBuffer():
    '''
    a class that wraps around a lvalert.out file produced by FakeDb
    provides some basic querying and manipulations for scripts that send lvalert messages
    '''

    def __init__(self, filenames):
        if isinstance(filenames, str):
            filenames = [filenames]
        self.fileMonitors = [FileMonitor(filename) for filename in filenames]

    def monitor(self, foo, cadence=0.1, *args, **kwargs):
        '''
        monitors the file, and when a change is detected we extract the call foo with signature:
        for node, message in self.extract():
            foo( node, message, *args, **kwargs )
        '''
        while True:
            for fileMonitor in self.fileMonitors:
                t = time.time()

                if fileMonitor.wasTouched():
                    fileMonitor.setTimestamp() ### update

                    for node, message in fileMonitor.extract():
                        foo( node, message, *args, **kwargs )

            wait = cadence - (time.time()-t)
            if wait:
                time.sleep(wait)

class FileMonitor():
    '''
    wraps around a file and knows how to monitor it for changes as well as extract those changes
    WARNING: holds an open file object in 'r' mode. This may cause issues if we have too many of these things...
    '''

    def __init__(self, filenames):
        if not os.path.exists(filename):
            raise ValueError('could not find filename=%s'%filename)
        self.filename = filename
        self.file_obj = open(filename, 'r')
        self.file_obj.seek(-1,2) ### go to end of file
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
        '''
        ans = self.file_obj.readline()
        nodeMessage = []
        while ans:
            nodeMessage.append( line2alert(ans) )
            ans = self.file_obj.readline()

        return nodeMessage
