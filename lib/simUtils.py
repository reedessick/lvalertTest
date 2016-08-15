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
                                    respondProb          = site_respondProb,
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
    if config.has_section('plot skymaps'):
        plotDelay  = config.getfloat('plot skymaps', 'plotSkymap delay')
        plotJitter = config.getfloat('plot skymaps', 'plotSkymap jitter')
        plotProb   = config.getfloat('plot skymaps', 'plotSkymap prob')
    else:
        plotDelay  = 0.0
        plotJitter = 0.0
        plotProb   = 0.0

    if config.has_section('skyviewer'):
        skyviewerDelay  = config.getfloat('skyviewer', 'skyviewer delay')
        skyviewerJitter = config.getfloat('skyviewer', 'skyviewer jitter')
        skyviewerProb   = config.getfloat('skyviewer', 'skyviewer prob')
    else:
        skyviewerDelay  = 0.0
        skyviewerJitter = 0.0
        skyviewerProb   = 0.0

    # bayestar
    if config.has_section('bayestar'):

        raise NotImplementedError('look for a WriteLog action in sched with os.path.basename(filename)=="psd.xml.gz"\nbump all bayestar actions so they start after that has been uploaded')

        lvem        = config.getboolean('bayestar', 'lvem')

        startDelay  = config.getfloat('bayestar', 'start delay')
        startJitter = config.getfloat('bayestar', 'start jitter')
        startProb   = config.getfloat('bayestar', 'start prob')

        finishDelay  = config.getfloat('bayestar', 'finish delay')
        finishJitter = config.getfloat('bayestar', 'finish jitter')
        finishProb   = config.getfloat('bayestar', 'finish prob')

        skymapDelay  = config.getfloat('bayestar', 'skymap delay')
        skymapJitter = config.getfloat('bayestar', 'skymap jitter')
        skymapProb   = config.getfloat('bayestar', 'skymap prob')

        bayestar = pe.Bayestar( graceDBevent,
                                gdb_url           = gdb_url,
                                startTimeout      = startDelay, 
                                startJitter       = startJitter, 
                                startProb         = startProb, 
                                skymapTimeout     = skymapDelay, 
                                skymapJitter      = skymapJitter, 
                                skymapProb        = skymapProb, 
                                finishTimeout     = finishDelay, 
                                finishJitter      = finishJitter, 
                                finishProb        = finishProb, 
                                plotSkymapTimeout = plotDelay, 
                                plotSkymapJitter  = plotJitter, 
                                plotSkymapProb    = plotProb, 
                                skyviewerTimeout  = plotDelay, 
                                skyviewerJitter   = plotJitter, 
                                skyviewerProb     = plotProb
                              )

        sched += bayestar.genSchedule(directory=directory, lvem=lvem)

    # lalinference
    if config.has_section('lalinference'):
        lvem        = config.getboolean('lalinference', 'lvem')

        startDelay  = config.getfloat('lalinference', 'start delay')
        startJitter = config.getfloat('lalinference', 'start jitter')
        startProb   = config.getfloat('lalinference', 'start prob')
        
        finishDelay  = config.getfloat('lalinference', 'finish delay')
        finishJitter = config.getfloat('lalinference', 'finish jitter')
        finishProb   = config.getfloat('lalinference', 'finish prob')

        skymapDelay  = config.getfloat('lalinference', 'skymap delay')
        skymapJitter = config.getfloat('lalinference', 'skymap jitter')
        skymapProb   = config.getfloat('lalinference', 'skymap prob')

        lalinf = pe.LALInference( graceDBevent,
                                  gdb_url           = gdb_url,
                                  startTimeout      = startDelay,
                                  startJitter       = startJitter,
                                  startProb         = startProb,
                                  skymapTimeout     = skymapDelay,
                                  skymapJitter      = skymapJitter,
                                  skymapProb        = skymapProb,
                                  finishTimeout     = finishDelay,
                                  finishJitter      = finishJitter,
                                  finishProb        = finishProb,
                                  plotSkymapTimeout = plotDelay,
                                  plotSkymapJitter  = plotJitter,
                                  plotSkymapProb    = plotProb,
                                  skyviewerTimeout  = plotDelay,
                                  skyviewerJitter   = plotJitter,
                                  skyviewerProb     = plotProb
                                )

        sched += lalinf.genSchedule(directory=directory, lvem=lvem)

    # lib
    if config.has_section('lib'):
        lvem        = config.getboolean('lalinference', 'lvem')

        startDelay  = config.getfloat('lib', 'start delay')
        startJitter = config.getfloat('lib', 'start jitter')
        startProb   = config.getfloat('lib', 'start prob')
        
        finishDelay  = config.getfloat('lib', 'finish delay')
        finishJitter = config.getfloat('lib', 'finish jitter')
        finishProb   = config.getfloat('lib', 'finish prob')

        skymapDelay  = config.getfloat('lib', 'skymap delay')
        skymapJitter = config.getfloat('lib', 'skymap jitter')
        skymapProb   = config.getfloat('lib', 'skymap prob')

        lib = pe.LIB( graceDBevent,
                      gdb_url           = gdb_url,
                      startTimeout      = startDelay,
                      startJitter       = startJitter,
                      startProb         = startProb,
                      skymapTimeout     = skymapDelay,
                      skymapJitter      = skymapJitter,
                      skymapProb        = skymapProb,
                      finishTimeout     = finishDelay,
                      finishJitter      = finishJitter,
                      finishProb        = finishProb,
                      plotSkymapTimeout = plotDelay,
                      plotSkymapJitter  = plotJitter,
                      plotSkymapProb    = plotProb,
                      skyviewerTimeout  = plotDelay,
                      skyviewerJitter   = plotJitter,
                      skyviewerProb     = plotProb
                    )

        sched += lib.genSchedule(directory=directory, lvem=lvem)

    # bayeswave
    if config.has_section('bayeswave'):
        lvem        = config.getboolean('lalinference', 'lvem')

        startDelay  = config.getfloat('bayeswave', 'start delay')
        startJitter = config.getfloat('bayeswave', 'start jitter')
        startProb   = config.getfloat('bayeswave', 'start prob')
        
        finishDelay  = config.getfloat('bayeswave', 'finish delay')
        finishJitter = config.getfloat('bayeswave', 'finish jitter')
        finishProb   = config.getfloat('bayeswave', 'finish prob')

        skymapDelay  = config.getfloat('bayeswave', 'skymap delay')
        skymapJitter = config.getfloat('bayeswave', 'skymap jitter')
        skymapProb   = config.getfloat('bayeswave', 'skymap prob')

        bayeswave = pe.Bayeswave( graceDBevent,
                                 gdb_url           = gdb_url,
                                 startTimeout      = startDelay,
                                 startJitter       = startJitter,
                                 startProb         = startProb,
                                 skymapTimeout     = skymapDelay,
                                 skymapJitter      = skymapJitter,
                                 skymapProb        = skymapProb,
                                 finishTimeout     = finishDelay,
                                 finishJitter      = finishJitter,
                                 finishProb        = finishProb,
                                 plotSkymapTimeout = plotDelay,
                                 plotSkymapJitter  = plotJitter,
                                 plotSkymapProb    = plotProb,
                                 skyviewerTimeout  = plotDelay,
                                 skyviewerJitter   = plotJitter,
                                 skyviewerProb     = plotProb
                               )

        sched += bayeswave.genSchedule(directory=directory, lvem=lvem)

    # cwbPE
    if config.has_section('cwbPE'):
        lvem        = config.getboolean('lalinference', 'lvem')

        finishDelay  = config.getfloat('cwbPE', 'finish delay')
        finishJitter = config.getfloat('cwbPE', 'finish jitter')
        finishProb   = config.getfloat('cwbPE', 'finish prob')

        skymapDelay  = config.getfloat('cwbPE', 'skymap delay')
        skymapJitter = config.getfloat('cwbPE', 'skymap jitter')
        skymapProb   = config.getfloat('cwbPE', 'skymap prob')

        cwbpe = pe.CoherentWaveBurst( graceDBevent,
                                      gdb_url           = gdb_url,
                                      skymapTimeout     = skymapDelay,
                                      skymapJitter      = skymapJitter,
                                      skymapProb        = skymapProb,
                                      finishTimeout     = finishDelay,
                                      finishJitter      = finishJitter,
                                      finishProb        = finishProb,
                                      plotSkymapTimeout = plotDelay,
                                      plotSkymapJitter  = plotJitter,
                                      plotSkymapProb    = plotProb,
                                      skyviewerTimeout  = plotDelay,
                                      skyviewerJitter   = plotJitter,
                                      skyviewerProb     = plotProb
                                    )

        sched += cwbpe.genSchedule(directory=directory, lvem=lvem)

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
