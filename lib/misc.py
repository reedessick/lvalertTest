description = "a module that simulates misc follow-up that isn't well grouped in other fields (eg: dq, pe)"
author = "reed.essick@ligo.org"

#-------------------------------------------------

'''
generate a different object for each follow-up. These may inherit from a single parent object, but they each should be able to produce data that would be uploaded to GraceDB
'''

class ExternalTriggers():
    def __init__(self):
        raise NotImplementedError

class UnblindInjections():
    def __init__(self):
        raise NotImplementedError

'''
def externalTriggers(gps, graceid, options):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['dt'])
        schedule.append( (gps+dt, 'Coincidence search complete', None, None) )
    return schedule

def unblindInjections(gps, graceid, options):
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['dt'])
        shedule.append( (gps+dt, 'No unblind injections in window', None, None) )
    return schedule
'''
