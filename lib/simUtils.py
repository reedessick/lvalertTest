description = "a module housing helper routines for simulateGraceDBEvent and simulateGraceDBStream"

#-------------------------------------------------

import numpy as np

import schedule

import pipelines
import humans
import dq
import pe
import misc

#-------------------------------------------------
### scheduling fake events
#-------------------------------------------------

def dt( rate, distrib="poisson" ):
    """
    draws the time between events based on distrib and rate
    distrib can be either "poisson" or "uniform"
    """
    if distrib=="poisson":
        return poisson_dt( rate )
    elif distrib=="uniform":
        return uniform_dt( rate )
    else:
        raise ValueError("distrib=%s not understood"%distrib)

def uniform_dt( rate ):
    return 1.0/rate

def poisson_dt( rate ):
    return - np.log(1.0 - np.random.rand()) / rate

#-------------------------------------------------
# generate a schedule for a single event
#-------------------------------------------------

def genSchedule(gps, far, instruments, config, safe=True, gdb_url='https://gracedb.ligo.org/api/', directory='.'):
    '''
    generates a schedule of actions for a single event based on the info in config
    '''
    graceDBevent = schedule.GraceDBEvent() ### needs to be passed to all objects which make schedules

    sched = schedule.Schedule()

    ### add schedule for event creation
    group = config.get('general', 'group')
    if safe and (group!='Test'):
        raise ValueError('in "safe" mode, we only allow group=Test')
    pipeline = config.get('general', 'pipeline')
    if config.has_option('general', 'search'):
        search = config.get('general', 'search')
    else:
        search = None

    pipeObj = pipelines.initPipeline(gps, far, instruments, group, pipeline, graceDBevent, search=search, gdb_url=gdb_url)
    sched += pipeObj.genSchedule(directory=directory)

    ### add schedule for human interactions
    if config.has_section('humans'):
        request = config.getboolean('humans', 'request')
        respond = config.getboolean('humans', 'respond')
        if request or respond:
            requestDelay   = config.getfloat('humans', 'request delay')
            requestJitter  = config.getfloat('humans', 'request jitter')

            site_respondDelay  = config.getfloat('humans', 'site respond delay')
            site_respondJitter = config.getfloat('humans', 'site respond jitter')
            site_respondProb   = config.getfloat('humans', 'site respond prob')
            site_successProb   = config.getfloat('humans', 'site success prob')

            adv_respondDelay  = config.getfloat('humans', 'adv respond delay')
            adv_respondJitter = config.getfloat('humans', 'adv respond jitter')
            adv_respondProb   = config.getfloat('humans', 'adv respond prob')
            adv_successProb   = config.getfloat('humans', 'adv success prob')

            ### request signoff from each participating site
            for ifo in instruments:
                site = humans.Site( ifo, 
                                    graceDBevent, 
                                    gdb_url              = gdb_url,
                                    requestTimeout       = requestDelay, 
                                    requestJitter        = requestJitter, 
                                    respondTimeout       = site_respondDelay, 
                                    respondJitter        = site_respondJitter, 
                                    respondProb          = site_respondProb
                                    respondProbOfSuccess = site_successProb, 
                                  )
                sched += site.genSchedule(request=request, respond=respond)

            ### EM Advocate responses
            adv = humans.Adv( graceDBevent, 
                              gdb_url              = gdb_url,
                              requestTimeout       = requestDelay,
                              requestJitter        = requestJitter,
                              respondTimeout       = adv_respondDelay,
                              respondJitter        = adv_respondJitter,
                              respondProb          = adv_respondProb,
                              respondProbOfSuccess = adv_successProb,
                            )
            sched += adv.genSchedule(request=request, respond=respond)

    ### add schedule for dq
    # idq
    if config.has_section('idq'):
        raise NotImplementedError('idq cannot be simulated')

    # segDB2grcDB
    if config.has_section('segDB2grcDB'):
        raise NotImplementedError('segDB2grcDB cannot be simulated')

    ### add schedule for pe
    # bayestar
    if config.has_section('bayestar'):
        raise NotImplementedError()

    # lalinference
    if config.has_section('lalinference'):
        raise NotImplementedError()

    # lib
    if config.has_section('lib'):
        raise NotImplementedError()

    # bayeswave
    if config.has_section('bayeswave'):
        raise NotImplementedError()

    # cwbPE
    if config.has_section('cwbPE'):
        raise NotImplementedError()

    ### add schedule for misc stuff
    # external triggers
    if config.has_section('external triggers'):
        timeout = config.getfloat('external triggers', 'delay')
        jitter  = config.getfloat('external triggers', 'jitter')
        respondProb = config.getfloat('external triggers', 'respond prob')
        successProb = config.getfloat('external triggers', 'success prob')

        exTrg = misc.ExternalTriggers( graceDBevent, 
                                       gdb_url       = gdb_url,
                                       timeout       = delay, 
                                       jitter        = jitter, 
                                       probOfReport  = respondProb, 
                                       probOfSuccess = successProb, 
                                     )
        sched += exTrg.genSchedule()

    # unblind injections
    if config.has_section('unblind injections'):
        timeout = config.getfloat('unblind injections', 'delay')
        jitter  = config.getfloat('unblind injections', 'jitter')
        respondProb = config.getfloat('unblind injections', 'respond prob')
        successProb = config.getfloat('unblind injections', 'success prob')

        unBld = misc.UnblindInjections( graceDBevent,
                                        gdb_url       = gdb_url,
                                        timeout       = delay,
                                        jitter        = jitter,
                                        probOfReport  = respondProb,
                                        probOfSuccess = successProb,
                                      ) 
        sched += unBld.genSchedule()

    ### we're done, so return the total schedule
    return sched
