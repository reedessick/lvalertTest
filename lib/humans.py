description = "a module that simulates how humans will interact with the automatic vetting process"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import schedule

#-------------------------------------------------

### define parent class

class HumanSignoff(object):
    '''
    a class that represents a human's signoff or more general interaction with the automatic vetting process
    '''
    name = 'human'

    def __init__(self, graceDBevent, respondTimeout=60.0, respondJitter=10.0, respondProbOfSuccess=1.0, requestTimeout=0.0, requestJitter=0.0, requestProbOfSuccess=1.0):
        self.graceDBevent = graceDBevent ### pointer to shared object that will contain graceid assigned to this event

        self.respondTimeout = timeout ### mean amount of time we wait
        self.respondJitter  = jitter  ### the stdv of the jitter aound self.timeout
        self.respondProb    = probOfSuccess ### probability of human returning OK

        ### repeate for request
        self.requestTimeout = requestTimeout
        self.requestJitter  = requestJitter
        self.requestProb    = requestProbOfSuccess

    def request(self):
        '''
        generate label for request
        '''
        return "%sREQ"%self.name

    def decide(self):
        '''
        flip a coinc and decide if we get an OK or a NO label
        '''
        if random.random() < self.prob: ### we succeed -> OK label
            return "%sOK"%(self.name)
        else: ### we reject -> NO label
            return "%sNO"%(self.name)

    def genSchedule(self, request=True, respond=True):
        '''
        generate a schedule for this activity
        '''
        sched = schedule.Schedule()
        if request:
            request_dt = max(0, random.normalvariate(self.requestTimeout, self.requestJitter)
            request = schedule.WriteLabel( request_dt, self.graceDBevent, self.request() )
            sched.insert( request )
        if respond:
            respond_dt = max(0, random.normalvariate(self.respondTimeout, self.respondJitter))
            if request:
                respond_dt = max(request_dt, respond_dt)
            respond = schedule.WriteLabel( respond_dt, self.graceDBevent, self.decide() )
            sched.insert( respond )
        return sched

#------------------------

### define daughter classes

class Site(humanSignoff):
    '''
    signoff from a particular site
    '''
    knownSites = ['H1', 'L1']

    def __init__(self, siteName, timeout=60.0, jitter=10.0, probOfSuccess=1.0):
        assert siteName in self.knownSites, 'siteName=%s is not in the list of known sites'%siteName ### ensure we know about this site
        self.name = siteName
        super(Site, self).__init__(timeout=timeout, jitter=jitter, probOfSuccess=probOfSuccess)

    def request(self):
        return "%sOPS"%self.name

class Adv(humanSignoff):
    '''
    signoff from EM Advocates
    '''
    self.name = 'ADV'
