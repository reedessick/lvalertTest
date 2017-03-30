description = "a module that simulates Parameter Estimation uploads to GraceDB"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import random

import schedule

#-------------------------------------------------

'''
generate a different object for each follow-up. These may inherit from a single parent object, but they each should be able to produce data that would be uploaded to GraceDB
'''

class Bayestar():
    def __init__(self, graceDBevent, startTimeout=10.0, startJitter=2.0, startProb=1.0, skymapTimeout=45.0, skymapJitter=5.0, skymapProb=1.0, finishTimeout=40.0, finishJitter=2.0, finishProb=1.0, plotSkymapTimeout=5.0, plotSkymapJitter=1.0, plotSkymapProb=1.0, skyviewerTimeout=5.0, skyviewerJitter=1.0, skyviewerProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startTimeout = startTimeout
        self.startJitter  = startJitter
        self.startProb    = startProb

        self.skymapTimeout = skymapTimeout
        self.skymapJitter  = skymapJitter
        self.skymapProb    = skymapProb

        self.finishTimeout = finishTimeout
        self.finishJitter  = finishJitter
        self.finishProb    = finishProb

        self.plotSkymapTimeout = plotSkymapTimeout
        self.plotSkymapJitter  = plotSkymapJitter
        self.plotSkymapProb    = plotSkymapProb

        self.skyviewerTimeout = skyviewerTimeout
        self.skyviewerJitter  = skyviewerJitter
        self.skyviewerProb    = skyviewerProb

    def writeFITS(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fitsname = "%s/bayestar.fits.gz"%dirname
        open(fitsname, 'w').close() ### may want to do more than this...
        return fitsname

    def genSchedule(self, directory='.', lvem=True):
        '''
        generate a schedule for Bayestar
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startTimeout, self.startJitter))
            for message in ['INFO:BAYESTAR:by your command...', 'INFO:BAYESTAR:starting sky localization']:
                sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            if random.random() < self.finishProb:
                finish_dt = max(start_dt, random.normalvariate(self.finishTimeout, self.finishJitter))

                message = 'INFO:BAYESTAR:sky localization complete'
                sched.insert( schedule.WriteLog( finish_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

                if random.random() < self.skymapProb:
                    skymap_dt = max(finish_dt, random.normalvariate(self.skymapTimeout, self.skymapJitter))
                    message = 'INFO:BAYESTAR:uploaded sky map'
                    fitsname = self.writeFITS(directory=directory)
                    tagname = ['sky_loc']
                    if lvem:
                        tagname.append( 'lvem' )
                    sched.insert( schedule.WriteLog( skymap_dt, self.graceDBevent, message, filename=fitsname, tagname=tagname, gdb_url=self.gdb_url ) )

                    ### add in plotting and skyviewer
                    agenda = PlotSkymaps(self.graceDBevent, timeout=self.plotSkymapTimeout, jitter=self.plotSkymapJitter, probOfSuccess=self.plotSkymapProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname) \
                             + Skyviewer(self.graceDBevent, timeout=self.skyviewerTimeout, jitter=self.skyviewerJitter, probOfSuccess=self.skyviewerProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname)
                    agenda.bump( skymap_dt )
                    sched += agenda

        return sched

class LALInference():
    def __init__(self, graceDBevent, startTimeout=10.0, startJitter=2.0, startProb=1.0, skymapTimeout=45.0, skymapJitter=5.0, skymapProb=1.0, finishTimeout=40.0, finishJitter=2.0, finishProb=1.0, plotSkymapTimeout=5.0, plotSkymapJitter=1.0, plotSkymapProb=1.0, skyviewerTimeout=5.0, skyviewerJitter=1.0, skyviewerProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startTimeout = startTimeout
        self.startJitter  = startJitter
        self.startProb    = startProb

        self.skymapTimeout = skymapTimeout
        self.skymapJitter  = skymapJitter
        self.skymapProb    = skymapProb

        self.finishTimeout = finishTimeout
        self.finishJitter  = finishJitter
        self.finishProb    = finishProb

        self.plotSkymapTimeout = plotSkymapTimeout
        self.plotSkymapJitter  = plotSkymapJitter
        self.plotSkymapProb    = plotSkymapProb

        self.skyviewerTimeout = skyviewerTimeout
        self.skyviewerJitter  = skyviewerJitter
        self.skyviewerProb    = skyviewerProb

    def writeFITS(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fitsname = "%s/lalinference_skymap.fits.gz"%dirname
        open(fitsname, 'w').close() ### may want to do more than this...
        return fitsname

    def writeDat(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        datname = "%s/posterior_samples.dat"%dirname
        open(datname, 'w').close() ### may want to do more than this...
        return datname

    def genSchedule(self, directory='.', lvem=True):
        '''
        generate a schedule for Bayestar
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startTimeout, self.startJitter))
            message = 'LALInference online estimation started'
            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            if random.random() < self.finishProb:
                finish_dt = max(start_dt, random.normalvariate(self.finishTimeout, self.finishJitter))

                message = 'LALInference online estimation finished'
                filename = self.writeDat(directory=directory)
                sched.insert( schedule.WriteLog( finish_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

                if random.random() < self.skymapProb:
                    skymap_dt = max(finish_dt, random.normalvariate(self.skymapTimeout, self.skymapJitter))
                    message = 'LALInference'
                    fitsname = self.writeFITS(directory=directory)
                    tagname = ['sky_loc']
                    if lvem:
                        tagname.append( 'lvem' )
                    sched.insert( schedule.WriteLog( skymap_dt, self.graceDBevent, message, filename=fitsname, tagname=tagname, gdb_url=self.gdb_url ) )

                    ### add in plotting and skyviewer
                    agenda = PlotSkymaps(self.graceDBevent, timeout=self.plotSkymapTimeout, jitter=self.plotSkymapJitter, probOfSuccess=self.plotSkymapProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname) \
                             + Skyviewer(self.graceDBevent, timeout=self.skyviewerTimeout, jitter=self.skyviewerJitter, probOfSuccess=self.skyviewerProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname)
                    agenda.bump( skymap_dt )
                    sched += agenda

        return sched

class LIB():
    def __init__(self, graceDBevent, startTimeout=10.0, startJitter=2.0, startProb=1.0, skymapTimeout=45.0, skymapJitter=5.0, skymapProb=1.0, finishTimeout=40.0, finishJitter=2.0, finishProb=1.0, plotSkymapTimeout=5.0, plotSkymapJitter=1.0, plotSkymapProb=1.0, skyviewerTimeout=5.0, skyviewerJitter=1.0, skyviewerProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startTimeout = startTimeout
        self.startJitter  = startJitter
        self.startProb    = startProb

        self.skymapTimeout = skymapTimeout
        self.skymapJitter  = skymapJitter
        self.skymapProb    = skymapProb

        self.finishTimeout = finishTimeout
        self.finishJitter  = finishJitter
        self.finishProb    = finishProb

        self.plotSkymapTimeout = plotSkymapTimeout
        self.plotSkymapJitter  = plotSkymapJitter
        self.plotSkymapProb    = plotSkymapProb

        self.skyviewerTimeout = skyviewerTimeout
        self.skyviewerJitter  = skyviewerJitter
        self.skyviewerProb    = skyviewerProb

    def writeFITS(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fitsname = "%s/LIB_skymap.fits.gz"%dirname
        open(fitsname, 'w').close() ### may want to do more than this...
        return fitsname

    def writeDat(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        datname = "%s/posterior_samples.dat"%dirname
        open(datname, 'w').close() ### may want to do more than this...
        return datname

    def genSchedule(self, directory='.', lvem=True):
        '''
        generate a schedule for Bayestar
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startTimeout, self.startJitter))
            message = "LIB Parameter estimation started."
            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            if random.random() < self.finishProb:
                finish_dt = max(start_dt, random.normalvariate(self.finishTimeout, self.finishJitter))

                message = 'LIB Parameter estimation finished'
                sched.insert( schedule.WriteLog( finish_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

                if random.random() < self.skymapProb:
                    skymap_dt = max(finish_dt, random.normalvariate(self.skymapTimeout, self.skymapJitter))
                    message = 'LIB'
                    fitsname = self.writeFITS(directory=directory)
                    tagname = ['sky_loc']
                    if lvem:
                        tagname.append( 'lvem' )
                    sched.insert( schedule.WriteLog( skymap_dt, self.graceDBevent, message, filename=fitsname, tagname=tagname, gdb_url=self.gdb_url ) )

                    ### add in plotting and skyviewer
                    agenda = PlotSkymaps(self.graceDBevent, timeout=self.plotSkymapTimeout, jitter=self.plotSkymapJitter, probOfSuccess=self.plotSkymapProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname) \
                             + Skyviewer(self.graceDBevent, timeout=self.skyviewerTimeout, jitter=self.skyviewerJitter, probOfSuccess=self.skyviewerProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname)
                    agenda.bump( skymap_dt )
                    sched += agenda

        return sched

class BayesWave():
    def __init__(self, graceDBevent, startTimeout=10.0, startJitter=2.0, startProb=1.0, skymapTimeout=45.0, skymapJitter=5.0, skymapProb=1.0, finishTimeout=40.0, finishJitter=2.0, finishProb=1.0, plotSkymapTimeout=5.0, plotSkymapJitter=1.0, plotSkymapProb=1.0, skyviewerTimeout=5.0, skyviewerJitter=1.0, skyviewerProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startTimeout = startTimeout
        self.startJitter  = startJitter
        self.startProb    = startProb

        self.skymapTimeout = skymapTimeout
        self.skymapJitter  = skymapJitter
        self.skymapProb    = skymapProb

        self.finishTimeout = finishTimeout
        self.finishJitter  = finishJitter
        self.finishProb    = finishProb

        self.plotSkymapTimeout = plotSkymapTimeout
        self.plotSkymapJitter  = plotSkymapJitter
        self.plotSkymapProb    = plotSkymapProb

        self.skyviewerTimeout = skyviewerTimeout
        self.skyviewerJitter  = skyviewerJitter
        self.skyviewerProb    = skyviewerProb

    def writeFITS(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fitsname = "%s/BW_skymap.fits"%dirname
        open(fitsname, 'w').close() ### may want to do more than this...
        return fitsname

    def genSchedule(self, directory='.', lvem=True):
        '''
        generate a schedule for Bayestar
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startTimeout, self.startJitter))
            message = 'BayesWaveBurst launched'
            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            if random.random() < self.finishProb:
                finish_dt = max(start_dt, random.normalvariate(self.finishTimeout, self.finishJitter))

                for message in ['BWB Follow-up results', 'BWB parameter estimation', 'BWB Bayes Factors']:
                    sched.insert( schedule.WriteLog( finish_dt, self.graceDBevent, message, tagname=['pe'], gdb_url=self.gdb_url ) )

                if random.random() < self.skymapProb:
                    skymap_dt = max(finish_dt, random.normalvariate(self.skymapTimeout, self.skymapJitter))
                    message = 'BWB'
                    fitsname = self.writeFITS(directory=directory)
                    tagname = ['sky_loc']
                    if lvem:
                        tagname.append( 'lvem' )
                    sched.insert( schedule.WriteLog( skymap_dt, self.graceDBevent, message, filename=fitsname, tagname=tagname, gdb_url=self.gdb_url ) )
                    
                    ### add in plotting and skyviewer
                    agenda = PlotSkymaps(self.graceDBevent, timeout=self.plotSkymapTimeout, jitter=self.plotSkymapJitter, probOfSuccess=self.plotSkymapProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname) \
                             + Skyviewer(self.graceDBevent, timeout=self.skyviewerTimeout, jitter=self.skyviewerJitter, probOfSuccess=self.skyviewerProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname)
                    agenda.bump( skymap_dt )
                    sched += agenda

        return sched

class CoherentWaveBurst():
    def __init__(self, graceDBevent, startTimeout=10.0, skymapTimeout=45.0, skymapJitter=5.0, skymapProb=1.0, finishTimeout=40.0, finishJitter=2.0, finishProb=1.0, plotSkymapTimeout=5.0, plotSkymapJitter=1.0, plotSkymapProb=1.0, skyviewerTimeout=5.0, skyviewerJitter=1.0, skyviewerProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.skymapTimeout = skymapTimeout
        self.skymapJitter  = skymapJitter
        self.skymapProb    = skymapProb

        self.finishTimeout = finishTimeout
        self.finishJitter  = finishJitter
        self.finishProb    = finishProb

        self.plotSkymapTimeout = plotSkymapTimeout
        self.plotSkymapJitter  = plotSkymapJitter
        self.plotSkymapProb    = plotSkymapProb

        self.skyviewerTimeout = skyviewerTimeout
        self.skyviewerJitter  = skyviewerJitter
        self.skyviewerProb    = skyviewerProb

    def writeFITS(self, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_randStr())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        fitsname = "%s/skyprobcc.fits.gz"%dirname
        open(fitsname, 'w').close() ### may want to do more than this...
        return fitsname

    def genSchedule(self, directory='.', lvem=True):
        '''
        generate a schedule for Bayestar
        '''
        sched = schedule.Schedule()
        if random.random() < self.finishProb:
            finish_dt = max(0, random.normalvariate(self.finishTimeout, self.finishJitter))

            message = 'cWB parameter estimation'
            sched.insert( schedule.WriteLog( finish_dt, self.graceDBevent, message, tagname=['pe'], gdb_url=self.gdb_url ) )

            if random.random() < self.skymapProb:
                skymap_dt = max(finish_dt, random.normalvariate(self.skymapTimeout, self.skymapJitter))
                message = 'cWB skymap fit'
                fitsname = self.writeFITS(directory=directory)
                tagname = ['sky_loc']
                if lvem:
                    tagname.append( 'lvem' )
                sched.insert( schedule.WriteLog( skymap_dt, self.graceDBevent, message, filename=fitsname, tagname=tagname, gdb_url=self.gdb_url ) )

                ### add in plotting and skyviewer
                agenda = PlotSkymaps(self.graceDBevent, timeout=self.plotSkymapTimeout, jitter=self.plotSkymapJitter, probOfSuccess=self.plotSkymapProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname) \
                         + Skyviewer(self.graceDBevent, timeout=self.skyviewerTimeout, jitter=self.skyviewerJitter, probOfSuccess=self.skyviewerProb, gdb_url=self.gdb_url).genSchedule(fitsname, tagname=tagname)
                agenda.bump( skymap_dt )
                sched += agenda

        return sched

#-----------

class PlotSkymaps():
    def __init__(self, graceDBevent, timeout=30.0, jitter=5.0, probOfSuccess=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.timeout = timeout
        self.jitter  = jitter
        self.prob    = probOfSuccess

    def genMessage(self, fits):
        return "Mollweide projection of %s"%fits

    def genPNG(self, fits):
        pngName = os.path.join( os.path.dirname(fits), os.path.basename(fits).split('.')[0]+".png" )
        open(pngName, "w").close() ### touch it so it exists
        return pngName

    def genSchedule(self, fits, tagname=['sky_loc']):
        sched = schedule.Schedule() 
        if random.random() < self.prob:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), self.graceDBevent, self.genMessage(fits), filename=self.genPNG(fits), tagname=tagname, gdb_url=self.gdb_url ) )
        return sched

class Skyviewer():
    def __init__(self, graceDBevent, timeout=30.0, jitter=5.0, probOfSuccess=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.timeout = timeout
        self.jitter  = jitter
        self.prob    = probOfSuccess

    def genMessage(self):
        return ''

    def genJSON(self, fits):
        if fits.endswith('.gz'):
            fits = fits[:-3]
        jsonName = fits[:-4]+"json"
        open(jsonName, "w").close() ### touch it so it exists
        return jsonName

    def genSchedule(self, fits, tagname=['sky_loc']):
        sched = schedule.Schedule()
        if random.random() < self.prob:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), self.graceDBevent, self.genMessage(), filename=self.genJSON(fits), tagname=tagname, gdb_url=self.gdb_url ) )
        return sched
