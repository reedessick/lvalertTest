description = "a module that simulates Parameter Estimation uploads to GraceDB"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import schedule

#-------------------------------------------------

'''
generate a different object for each follow-up. These may inherit from a single parent object, but they each should be able to produce data that would be uploaded to GraceDB
'''

class Bayestar():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        raise NotImplementedError

class LALInference():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        raise NotImplementedError

class LIB():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        raise NotImplementedError

class BayesWave():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        raise NotImplementedError

class CoherentWaveBurst():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        raise NotImplementedError

'''
def bayestar(gps, graceid, options, skymapOptions={}):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['start dt'])
        schedule.append( (gps+dt, 'INFO:BAYESTAR:starting sky localization', None, None) )

        dt = float(options['skymap dt'])
        filename = 'bayestar.fits.gz'
        schedule.append( (gps+dt, 'INFO:BAYESTAR:uploaded sky map', filname, ['sky_loc']) )
        if skymapOptions.has_key('plotSkymaps'):
            schedule += plotSkymaps( gps+dt, graceid, skymapOptions['plot skymaps'], fits=filename )
        if skymapOptions.has_key('skyviewer'):
            schedule += skyviewer( gps+dt, graceid, skymapOptions['skyviewer'], fits=filename )

        dt = float(options['finish dt'])
        schedule.append( (gps+dt, 'INFO:BAYESTAR:sky localization complete', None, None) )

    return schedule

def lalinf(gps, graceid, options, skymapOptions={}):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['start dt'])
        schedule.append( (gps+dt, 'LALInference online estimation started', None, ['pe']) )

        dt = float(options['skymap dt'])
        filename = 'LALInference_skymap.fits.gz'
        touch( filename )
        schedule.append( (gps+dt, 'LALInference', filename, ['sky_loc']) )
        if skymapOptions.has_key('plotSkymaps'):
            schedule += plotSkymaps( gps+dt, graceid, skymapOptions['plot skymaps'], fits=filename )
        if skymapOptions.has_key('skyviewer'):
            schedule += skyviewer( gps+dt, graceid, skymapOptions['skyviewer'], fits=filename )

        dt = float(options['post samp dt'])
        filename = 'posterior_samples.dat'
        touch( filename )
        schedule.append( (gps+dt, 'LALInference online estimation finished', filename, ['pe']) )

    return schedule

def lib(gps, graceid, options, skymapOptions={}):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['start dt'])
        schedule.append( (gps+dt, 'LIB Parameter estimation started.', None, ['pe']) )

        dt = float(options['bayes factor dt'])
        schedule.append( (gps+dt, 'LIB PE summary', None, ['pe']) )

        dt = float(options['skymap dt'])
        filename = 'LIB_skymap.fits.gz'
        touch( filename )
        schedule.append( (gps+dt, 'LIB', filename, ['sky_loc']) )
        if skymapOptions.has_key('plotSkymaps'):
            schedule += plotSkymaps( gps+dt, graceid, skymapOptions['plot skymaps'], fits=filename )
        if skymapOptions.has_key('skyviewer'):
            schedule += skyviewer( gps+dt, graceid, skymapOptions['skyviewer'], fits=filename )

        dt = float(options['post samp dt'])
        filename = 'posterior_samples.dat'
        touch( filename )
        schedule.append( (gps+dt, 'LIB Parameter estimation finished', filename, ['pe']) )

    return schedule

def bayeswave(gps, graceid, options, skymapOptions={}):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['start dt'])
        schedule.append( (gps+dt, 'BayesWaveBurst launched', None, ['pe']) )

        dt = float(options['post samp dt'])
        schedule.append( (gps+dt, 'BWB Follow-up results', None, ['pe']) )

        dt = float(options['estimate dt'])
        schedule.append( (gps+dt, 'BWB parameter estimation', None, ['pe']) )

        dt = float(options['bayes factor dt'])
        schedule.append( (gps+dt, 'BWB Bayes Factors', None, ['pe']) )

        dt = float(options['skymap dt'])
        filename = 'BW_skymap.fits'
        schedule.append( (gps+dt, 'BWB', filename, ['sky_loc']) )
        if skymapOptions.has_key('plotSkymaps'):
            schedule += plotSkymaps( gps+dt, graceid, skymapOptions['plot skymaps'], fits=filename )
        if skymapOptions.has_key('skyviewer'):
            schedule += skyviewer( gps+dt, graceid, skymapOptions['skyviewer'], fits=filename )

    return schedule

'''

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

    def genPNG(self, fits, directory='.'):
        pngName = os.path.join(directory, fits.split('.')[0]+".png")
        open(pngName,"w").close()
        return pngName

    def genSchedule(self, fits, directory='.', tagname=['sky_loc']):
        sched = schedule.Schedule() 
        if random.random() < self.prob:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), self.graceDBevent, self.genMessage(fits), filename=self.genPNG(fits, directory=directory), tagname=tagname, gdb_url=self.gdb_url ) )
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

    def genJSON(self, fits, directory='.'):
        jsonName = os.path.join(directory, fits.strip('.gz').strip("fits")+"json")
        open(jsonName, "w").close() ### touch it so it exists
        return jsonName

    def genSchedule(self, fits, directory='.', tagname=['sky_loc']):
        sched = schedule.Schedule()
        if random.random() < self.prob:
            sched.insert( schedule.WriteLog( max(0, random.normalvariate(self.timeout, self.jitter)), self.graceDBevent, self.genMessage(), filename=self.genJSON(fits, directory=directory), tagname=tagname, gdb_url=self.gdb_url ) )
        return sched
