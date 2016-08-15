description = "a module that simulates Data Quality products"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import schedule

#-------------------------------------------------

class SegDB2GrcDB():
    def __init__(self, graceDBevent, flags=[], startDelay=10, startJitter=1, startProb=1.0, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.startDelay  = startDelay
        self.startJitter = startJitter
        self.startProb   = startProb

        self.flags = flags

    def genFilename(self, flag, start, dur, directory='.'):
        dirname = "%s/%s/"%(directory, self.graceDBevent.get_graceid())
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        filename = "%s/%s-%d-%d.xml"%(dirname, flag.replace(":","_"), start, dur)
        open(filename, 'w').close() ### may want to do more than this...
        return filename

    def genSchedule(self, directory='.')
        '''
        generate a schedule for SegDB2GraceDB uploads
        '''
        sched = schedule.Schedule()
        if random.random() < self.startProb:
            start_dt = max(0, random.normalvariate(self.startDelay, self.startJitter))
            message = 'began searching for segments in : fakeSegDB'
            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

            for flag, (delay, jitter, prob), (start, dur) in flags:
                if random.random() < prob:
                    filename = self.genFilename( flag, start, dur, directory=directory )
                    start_time += max(0, random.normalvariate(delay, jitter))
                    sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, flag, filename=filename, gdb_url=self.gdb_url ) )
                else:
                    break ### process failed, so we stop

            sched.insert( schedule.WriteLog( start_dt, self.graceDBevent, 'finished searching for segments in : fakeSegDB', gdb_url=self.gdb_url ) )

        return sched

class IDQ():
    def __init__(self, graceDBevent, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

    def genSchedule(self, directory='.'):
        raise NotImplementedError










'''
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
'''
