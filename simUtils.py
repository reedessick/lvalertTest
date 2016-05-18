description = "a module housing helper routines for simulateGraceDBEvent and simulateGraceDBStream"

import numpy as np

from lal.gpstime import tconvert

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
    return 1/rate

def poisson_dt( rate ):
    return - np.log(1 - np.random.rand()) / rate

#-------------------------------------------------
# general utils
#-------------------------------------------------

def touch( filename ):
    open(filename, 'w').close()

#-------------------------------------------------
### fake data and messages to dummy follow-up
# these return [(dt, message, filename, tagnames)]
#-------------------------------------------------
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

def idq(gps, graceid, options):
    schedule = []
    ifos = options['ifos'].split()
    classifiers = options['classifiers'].split()
    start = gps-30
    end = gps+30
    dur = 60

    for ifo in ifos:
        if np.random.rand() <= float(options['prob of success']):

            dt = float(options['start dt'])
            schedule.append( (gps+dt, 'Started Searching for iDQ information within [%d, %d] at %s'%(start, end, ifo), None, None) )

            dt = float(options['tables dt'])
            for classifier in classifiers:
                filename = "%s_idq_%s-%d-%d.xml.gz"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ glitch tables %s:'%(ifo), filename, ['data_quality']) )

            dt = float(options['glitch fap dt'])
            for classifier in classifiers:
                filename = "%s_%s-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'minimum glitch-FAP for %s at %s within [%d, %d] is %.6f'%(classifier, ifo, start, end, np.random.rand()), filename, ['data_quality']) )

            dt = float(options['fap frame dt'])
            for classifier in classifiers:
                filename = "%s_idq_%s_fap-%d-%d.gwf"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ fap timesereis for %s at %s within [%d, %d] :'%(classifier, ifo, start, end), filename, ['data_quality']) )

            dt = float(options['rank frame dt'])
            for classifier in classifiers:
                filename = "%s_idq_%s_rank-%d-%d.gwf"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ glitch-rank frame for %s at %s within [%d, %d] :'%(classifier, ifo, start, end), filename, ['data_quality']) )

            dt = float(options['timeseries plot dt'])
            for classifeir in classifiers:
                filename = "%s_%s_timeseries-%d-%d.png"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ fap and glitch-rank timeseries plot for %s at %s:'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['active chan dt'])
            for classifier in classifiers:
                filename = "%s_%s_chanlist-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ (possible) active channels for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['active chan plot dt'])
            for classifier in classifiers:
                filename = "%s_%s_chanstrip-%d-%d.png"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ channel strip chart for %s at %s'%(classifier, ifo)) )

            dt = float(options['calib dt'])
            for classifier in classifiers:
                filename = "%s_%s_calib-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ calibration sanity check for %s at %s', filename, ['data_quality']) )

            dt = float(options['calib plot dt'])
            for classifier in classifiers:
                filename = "%s_%s_calib-%d-%d.png"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ calibration sanity check figure for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['roc dt'])
            for classifier in classifiers:
                filename = "%s_%s_ROC-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ local ROC curves for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['roc plot dt'])
            for classifier in classifiers:
                filename = "%s_%s_ROC-%d-%d.png"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ local ROC figure for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['calib stats dt'])
            for classifier in classifiers:
                filename = "%s_%s_calibStats-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ local calibration vital statistics for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['train stats dt'])
            for classifier in classifiers:
                filename = "%s_%s_trainStats-%d-%d.json"%(ifo, classifier, start, dur)
                touch( filename )
                schedule.append( (gps+dt, 'iDQ local training vital statistics for %s at %s'%(classifier, ifo), filename, ['data_quality']) )

            dt = float(options['finish dt'])
            schedule.append( (gps+dt, 'Finished searching for iDQ information within [%d, %d] at %s'%(start, end, ifo), None, None) )

    return schedule

def segDB2grcDB(gps, graceid, options):
    schedule = []
    if np.random.rand() <= float(options['prob of success']):
        dt = float(options['start dt'])
        schedule.append( (gps+dt, 'began searching for segments in : fakeDB', None, None) )

        dt = float(options['flags dt'])
        start = int(gps)-30
        dur = 60
        for flags in options['flags'].split():
            filename = "%s-%d-%d.xml"%(flag.replace(":","_"), start, dur)
            touch( filename )
            schedule.append( (gps+dt, flag, filename, None) )

        dt = float(options['finish dt'])
        schedule.append( (gps+dt, 'finished searching for segments in : fakeDB', None, None) ) 

    return schedule

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

#-------------------------------------------------
### dummy up event creation
#-------------------------------------------------
def eventCreation( gps, far, pipeline ):
    """
    generates the necessary dummy files for event submission
    """
    if pipeline=="cwb":
        filename = cwb_initialData( gps, far )
        return filename, [('cWB CED', None, None)]

    if pipeline=="lib":
        filename = lib_initialData( gps, far )
        return filename, []

    if pipeline in ["gstlal", "mbtaonline", "pycbc", "gstlal-spiir"]:
        filename = coinc_initialData( gps, far )
        psdname = psd_Data( gps )
        return filename, [('amplitude spectral densities', psdname, None)]

def cwb_initialData( gps, far ):
    """
    writes file and returns the path
    pretty much all the parameters supplied here ar bullshit, but gps and far are set individually for each event
    """
    filename = "trigger_%.4f.txt"%gps
    file_obj = open(filename, "w")
    s = """nevent:     1
ndim:       2
run:        1
name:
rho:        6.135850
netCC:      0.789861
netED:      0.013624
penalty:    0.644564
gnet:       0.713356
anet:       0.420364
inet:       1.918868
likelihood: 1.078950e+02
ecor:       5.091192e+01
ECOR:       5.091192e+01
factor:     0.000000
range:      0.000000
mchirp:     0.000000
norm:       0.709433
usize:      0
ifo:        L1 H1
eventID:    1 0
rho:        6.135850 6.388615
type:       1 0
rate:       0 0
volume:     19 19
size:       5 12
lag:        0.000000 0.000000
slag:       0.000000 0.000000
phi:        86.132812 0.000000 331.512665 188.789062
theta:      124.228867 0.000000 -34.228867 107.582779
psi:        -41.259022 0.000000
iota:       33.768776 0.000000
bp:          0.7775 -0.7217
inj_bp:      0.0000  0.0000
bx:          0.6239 -0.5740
inj_bx:      0.0000  0.0000
chirp:      0.000000 0.000022 0.013721 0.994642 0.800000 0.627830
range:      0.000000 0.000000
Deff:       0.000000 0.000000
mass:       0.000000 0.000000
spin:       0.000000 0.000000 0.000000 0.000000 0.000000 0.000000
eBBH:       0.000000 0.000000 0.000000
null:       5.746222e-01 8.710215e-01
strain:     2.197399e-22 0.000000e+00
hrss:       1.525938e-22 1.581162e-22
inj_hrss:   0.000000e+00 0.000000e+00
noise:      1.948514e-23 2.317232e-23
segment:    1137313332.0000 1137313528.0000 1137313332.0000 1137313528.0000
start:      1137313504.6875 1137313504.6875
time:       %(gps).4f %(gps).4f
stop:       1137313504.9531 1137313504.9531
inj_time:         0.0000       0.0000
left:       172.687500 172.687500
right:      23.046875 23.046875
duration:   0.040769 0.265625
frequency:  1778.375732 1776.344116
low:        1696.000000
high:       1840.000000
bandwidth:  25.198294 144.000000
snr:        6.258973e+01 4.643370e+01
xSNR:       6.192430e+01 4.653129e+01
sSNR:       6.126594e+01 4.662908e+01
iSNR:       0.000000 0.000000
oSNR:       0.000000 0.000000
ioSNR:      0.000000 0.000000
netcc:      0.789861 0.805347 0.720986 0.528434
neted:      0.693614 6.445643 109.023430 209.443497 218.316010
erA:         0.000  8.618 12.818 16.623 20.506 24.510 29.298 35.372 43.102 55.354  0.000
sky_res:    0.458065
map_lenght: 196587
Qveto:      48.586632 41.129078
wat version = 2G
online version = 1
search=r
## number of events with rho>= rho of the event | corresponding rate | start time of the analyzed segment list | end time of the analyzed segment list | duration of the analyzed segment list
#significance based on the last processed job (about 180 seconds)
1 %(far).9f 1126799464 1126800064 600
#significance based on the last hour
38 %(far).9f 1137246076 1137250424 4348
#significance based on the last day
5218 %(far).9f 1136639504 1137250424 610920
#significance based on all the processed livetime in the run
282527 %(far).9f 1126595840 1137025824 10429984
/home/drago/online/O1_LH_ONLINE/JOBS/113731/1137313460-1137313520/OUTPUT.merged/TRIGGERS/trigger_1137313504.8337.txt"""%{'gps':gps, 'far':far}
    print >> file_obj, s
    file_obj.close()

    return filename

def lib_initialData( gps, far):
    """
    writes file and returns the path
    pretty much all the parameters supplied here ar bullshit, but gps and far are set individually for each event
    """
    filename = "%.2f-0.json"%(gps)
    file_obj = open(filename, "w")
    d = {
        "gpstime": "%.2f"%gps, 
        "BCI": 656791.97499999998, 
        "quality": 4.1737469689720612, 
        "FAR": far, 
        "instruments": "H1,L1", 
        "BSN": 4617639.6268060002, 
        "Omicron SNR": 91.166239947899996, 
        "hrss": 3.245759049352791e-19, 
        "nevents": 1, 
        "timeslides": {"H1": "0.0", "L1": "0.0"}, 
        "frequency": 32.000034917039528, 
        "likelihood": null
        }
    file_obj.write( json.dumps(d) )
    file_obj.close()
    return filename

def coinc_initialData( gps, far):
    """
    writes file and returns the path
    pretty much all the parameters supplied here ar bullshit, but gps and far are set individually for each event
    """
    raise NotImplementedError

def psd_initialData( gps, far):
    """
    writes file and returns the path
    pretty much all the parameters supplied here ar bullshit, but gps and far are set individually for each event
    """
    raise NotImplementedError
