description = "a module housing basic utilities for scheduling actions and uploads into GraceDb"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import time

#-------------------------------------------------

class GraceDBEvent(object):
    '''
    a wrapper around a single attribute (graceid) so that different actions can reference the same event without know what the graceid is a priori
    '''
    def __init__(self, graceid=None):
        self.graceid = None

    def get_graceid(self):
        if self.graceid!=None:
            return self.graceid
        else:
            raise RuntimeError('graceid has not been set for this GraceDBEvent yet!')

    def set_graceid(self, graceid):
        if self.graceid==None:
            self.graceid = graceid
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

    def setExpiration(self, t0):
        self.expiration = t0+self.dt

    def hasExpired(self):
        return time.time() > self.expiration

    def wait(self):
        if not self.hasExpired():
            time.sleep(time.time()-self.expiration)

    def execute(self):
        self.foo( *self.args, **self.kwargs )

class Schedule(object):
    '''
    an ordered list of Actions
    '''
    def __init__(self):
        self.actions = []

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
                self.actions.append( newAction )

    def pop(self, ind=0):
        return actions.pop(ind)

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

#-------------------------------------------------

class CreateEvent(Action):
    '''
    create an event
    '''
    def __init__(self, dt, graceDBevent, group, pipeline, filename, search=None, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBentry = graceDBevent
        self.gdb_url = gdb_url

        self.filename = filename
        self.group    = group
        self.pipeline = pipeline

        super(Label, self).__init__(dt, self.createEvent)

    def createEvent(self, *args, **kwargs):
        '''
        creates the entry in the database and updates self.graceDBevent.graceid so that other Actions know which graceid was assigned
        '''
        gdb = GraceDb(self.gdb_url)
        http_response = gdb.createEvent( self.group, self.pipeline, self.filename, search=self.search )
        self.graceDBevent.set_graceid( json.loads( http_response.read() )['graceid'] ) 

class WriteLabel(Action):
    '''
    apply a label
    '''
    def __init__(self, dt, graceDBevent, label, gdb_url='https://gracedb.ligo.org/api'):
        self.graceDBentry = graceDBevent
        self.gdb_url = gdb_url

        self.label = label

        super(Label, self).__init__(dt, self.writeLabel)

    def writeLabel(self, *args, **kwargs):
        gdb = GraceDb(self.gdb_url)
        gdb.writeLabel( self.graceDBevent.get_graceid(), self.label )

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
      
        super(Log, self).__init__(dt, writeLog)

    def writeLog(self, *args, **kwargs):
        gdb = GraceDb(self.gdb_url)
        gdb.writeLog( self.graceDBevent.get_graceid(), self.message, filename=self.filename, tagname=self.tagname )
