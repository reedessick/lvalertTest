description = "a module that simulates misc follow-up that isn't well grouped in other fields (eg: dq, pe)"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import schedule

#-------------------------------------------------

'''
generate a different object for each follow-up. These may inherit from a single parent object, but they each should be able to produce data that would be uploaded to GraceDB
'''

class ExternalTriggers():
    def __init__(self, graceDBevent, timeout=60.0, jitter=10.0, probOfReport=1.0, probOfSuccess=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.timeout = timeout
        self.jitter  = jitter
        self.probOfReport  = probOfReport
        self.probOfSuccess = probOfSuccess

    def genMessage(self):
        if random.random() < self.probOfSuccess:
            return "Coincidence search complete"
        else:
            raise NotImplementedError

    def genSchedule(self):
        sched = schedule.Schedule()
        if random.random() < self.probOfReport:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), 
                                             self.graceDBevent, 
                                             self.genMessage(), 
                                             gdb_url=self.gdb_url, 
                                           ) 
                        )
        return sched

class UnblindInjections():
    def __init__(self, graceDBevent, timeout=60.0, jitter=10.0, probOfReport=1.0, probOfSuccess=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.timeout = timeout
        self.jitter  = jitter
        self.probOfReport  = probOfReport
        self.probOfSuccess = probOfSuccess

    def genMessage(self):
        if random.random() < self.probOfSuccess:
            return "No unblind injections in window"
        else:
            raise NotImplementedError

    def genSchedule(self):
        sched = schedule.Schedule()
        if random.random() < self.probOfReport:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), 
                                             self.graceDBevent, 
                                             self.genMessage(), 
                                             gdb_url=self.gdb_url,
                                           ) 
                        )
        return sched
