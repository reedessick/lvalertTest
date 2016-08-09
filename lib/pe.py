description = "a module that simulates Parameter Estimation uploads to GraceDB"
author = "reed.essick@ligo.org"

#-------------------------------------------------

'''
generate a different object for each follow-up. These may inherit from a single parent object, but they each should be able to produce data that would be uploaded to GraceDB
'''

class Bayestar():
    def __init__(self):
        raise NotImplementedError

class LALInference():
    def __init__(self):
        raise NotImplementedError

class LIB():
    def __init__(self):
        raise NotImplementedError

class BayesWave():
    def __init__(self):
        raise NotImplementedError

class CoherentWaveBurst():
    def __init__(self):
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
    def __init__(self):
        raise NotImplementedError

class Skyviewer():
    def __init__(self):
        raise NotImplementedError

'''
def plotSkymaps(gps, graceid, options, fits='fake.fits.gz'):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['dt'])
        filename = fits.split('.')[0]+".png"
        schedule.append( (gps+dt, 'Mollweide projection of '+fits, filename, ['sky_loc']) )

    return schedule

def skyviewer(gps, graceid, options, fits='fake.fits.gz'):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['dt'])
        filename = fits.strip('.gz').strip('fits')+'json'
        schedule.append( (gps+dt, '', filename, ['sky_loc']) )

    return schedule
'''
