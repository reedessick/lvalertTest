description = "a module housing basic utilities for scheduling actions and uploads into GraceDb"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import time

import json

from ligo.gracedb.rest import GraceDb

#-------------------------------------------------

class GraceDBEvent(object):
    '''
    a wrapper around a single attribute (graceid) so that different actions can reference the same event without know what the graceid is a priori
    '''
    def __init__(self, graceid=None):
        self.__graceid__ = None

    def __str__(self):
        return "graceid : "+str(self.__graceid__)

    def get_graceid(self, force=False):
        if force or (self.__graceid__!=None):
            return self.__graceid__
        else:
            raise RuntimeError('graceid has not been set for this GraceDBEvent yet!')

    def set_graceid(self, graceid, force=False):
        if force or (self.__graceid__==None):
            self.__graceid__ = graceid
        else:
            raise RuntimeError('graceid has already been set for this GraceDBEvent!')

#-------------------------------------------------

class Action(object):
    '''
    a thing that should be done. 
    '''
    def __init__(self, dt, foo, *args, **kwargs):
        self.dt     = dt ### how long we wait, used to order these within Schedule
        self.foo    = foo ### a function handle for what we do
        self.args   = args ### arguments for the function handle
        self.kwargs = kwargs ### keyword arguments for function handle

        self.expiration = None

    def __str__(self):
        return """Action
    timeout : %.3f
    expiration : %.3f"""%(self.dt, self.expiration)

    def setExpiration(self, t0):
        self.expiration = t0+self.dt

    def hasExpired(self):
        return time.time() > self.expiration

    def wait(self, verbose=False):
        if not self.hasExpired():
            wait = self.expiration - time.time()
        else:
            wait = 0.0
        if verbose:
            print "sleeping for %.3f sec"%wait
        time.sleep( wait )

    def execute(self):
        self.foo( *self.args, **self.kwargs )

class Schedule(object):
    '''
    an ordered list of Actions
    '''
    def __init__(self):
        self.actions = []

    def __iter__(self):
        return self.actions.__iter__()

    def insert(self, newActions):
        if not hasattr( newActions, "__iter__"):
            newActions = [newActions]
        newActions.sort(key=lambda a: a.dt)

        ind = 0
        while ind < len(self.actions):
            action = self.actions[ind]
            while len(newActions):
                newAction = newActions[0]
                if newAction.dt < action.dt:
                    self.actions.insert(ind, newActions.pop(0))
                    ind += 1
                else:
                    break
            ind += 1
        else:
            while len(newActions):
                self.actions.append( newActions.pop(0) )

    def pop(self, ind=0):
        return self.actions.pop(ind)

    def __len__(self):
        return len(self.actions)

    def __add__(self, other):
        '''creates a new instance'''
        newSchedule = Schedule()
        for schedule in [self, other]:
            while len(schedule):
                newSchedule.insert( schedule.pop() )
        return newSchedule

    def bump(self, delay):
        '''increases expiration of all contained actions by delay'''
        for action in self.actions:
            action.expiration += delay

    def setExpiration(self, t0):
        for action in self.actions:
            action.setExpiration( t0 )

#-------------------------------------------------

class CreateEvent(Action):
    '''
    create an event
    '''
    def __init__(self, dt, graceDBevent, group, pipeline, filename, search=None, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBevent = graceDBevent
        self.gdb_url = gdb_url

        self.filename = filename
        self.group    = group
        self.pipeline = pipeline
        self.search   = search

        super(CreateEvent, self).__init__(dt, self.createEvent)

    def __str__(self):
        return """CreateEvent -> %s
    group      : %s
    pipeline   : %s
    search     : %s
    filename   : %s
    timeout    : %.3f
    expiration : %.3f"""%(self.gdb_url, self.group, self.pipeline, self.search, self.filename, self.dt, self.expiration)

    def createEvent(self, *args, **kwargs):
        '''
        creates the entry in the database and updates self.graceDBevent.graceid so that other Actions know which graceid was assigned
        '''
        gdb = GraceDb(self.gdb_url)
        httpResponse = gdb.createEvent( self.group, self.pipeline, self.filename, search=self.search )
        httpString = httpResponse.read()

        print httpString

        self.graceDBevent.set_graceid( json.loads( httpString )['graceid'] ) 

class WriteLabel(Action):
    '''
    apply a label
    '''
    def __init__(self, dt, graceDBevent, label, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBevent = graceDBevent
        self.gdb_url = gdb_url

        self.label = label

        super(WriteLabel, self).__init__(dt, self.writeLabel)

    def __str__(self):
        return """WriteLabel -> %s
    graceid    : %s
    label      : %s
    timeout    : %.3f
    expiration : %.3f"""%(self.gdb_url, self.graceDBevent.get_graceid(force=True), self.label, self.dt, self.expiration)

    def writeLabel(self, *args, **kwargs):
        gdb = GraceDb(self.gdb_url)
        httpResponse = gdb.writeLabel( self.graceDBevent.get_graceid(), self.label )

        print httpResponse.read()

class RemoveLabel(Action):
    '''
    remove a label
    '''
    def __init__(self, dt, graceDBevent, label, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBevent = graceDBevent
        self.gdb_url = gdb_url

        self.label = label

        super(RemoveLabel, self).__init__(dt, self.removeLabel)

    def __str__(self):
        return """RemoveLabel -> %s
    graceid    : %s
    label      : %s
    timeout    : %.3f
    expiration : %.3f"""%(self.gdb_url, self.graceDBevent.get_graceid(force=True), self.label, self.dt, self.expiration)

    def removeLabel(self, *args, **kwargs):
        gdb = GraceDb(self.gdb_url)
        httpResponse = gdb.removeLabel( self.graceDBevent.get_graceid(), self.label )

        print httpResponse.read()

class WriteLog(Action):
    '''
    write a log message
    '''
    def __init__(self, dt, graceDBevent, message, filename=None, tagname=None, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBevent = graceDBevent
        self.gdb_url = gdb_url

        self.message = message
        self.filename = filename
        self.tagname = tagname
      
        super(WriteLog, self).__init__(dt, writeLog)

    def __str__(self):
        return """WriteLog -> %s
    graceid    : %s
    message    : %s
    filename   : %s
    tagname    : %s
    timeout    : %.3f
    expiration : %.3f"""%(self.gdb_url, self.graceDBevent.get_graceid(force=True), self.message, self.filename, self.tagname, self.dt, self.expiration)

    def writeLog(self, *args, **kwargs):
        gdb = GraceDb(self.gdb_url)
        httpResponse = gdb.writeLog( self.graceDBevent.get_graceid(), self.message, filename=self.filename, tagname=self.tagname )

        print httpResponse.read()
