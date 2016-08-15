description = "a module that simulates Data Quality products"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import random

import numpy as np

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

    def genSchedule(self, directory='.'):
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
    def __init__(self, 
                 graceDBevent, 
                 instruments,
                 classifiers,
                 maxFAP = 1.0,
                 minFAP = 1e-5,
                 gdb_url = 'https://gracedb.ligo.org/api/',
                 startDelay  = 1.0,
                 startJitter = 0.5,
                 startProb   = 1.0,
                 tablesDelay  = 10.0,
                 tablesJitter = 1.0,
                 tablesProb   = 1.0,
                 fapDelay  = 5.0,
                 fapJitter = 1.0,
                 fapProb   = 1.0,
                 gwfDelay  = 5.0,
                 gwfJitter = 1.0,
                 gwfProb   = 1.0,
                 timeseriesDelay  = 5.0,
                 timeseriesJitter = 1.0,
                 timeseriesProb   = 1.0,
                 activeChanDelay  = 10, 
                 activeChanJitter = 1.0,
                 activeChanProb   = 1.0,
                 calibDelay  = 20.,
                 calibJitter = 5,
                 calibProb   = 1.0,
                 rocDelay  = 20,
                 rocJitter = 5,
                 rocProb   = 1.0,
                 statsDelay  = 30,
                 statsJitter = 5,
                 statsProb   = 1.0,
                ):

        ### store variables
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        self.classifiers = classifiers
        self.instruments = instruments

        self.minFAP = minFAP
        self.maxFAP = maxFAP

        self.startDelay  = startDelay
        self.startJitter = startJitter
        self.startProb   = startProb

        self.tablesDelay  = tablesDelay
        self.tablesJitter = tablesJitter
        self.tablesProb   = tablesProb

        self.fapDelay  = fapDelay
        self.fapJitter = fapJitter
        self.fapProb   = fapProb

        self.gwfDelay  = gwfDelay
        self.gwfJitter = gwfJitter
        self.gwfProb   = gwfProb

        self.timeseriesDelay  = timeseriesDelay
        self.timeseriesJitter = timeseriesJitter
        self.timeseriesProb   = timeseriesProb

        self.activeChanDelay  = activeChanDelay
        self.activeChanJitter = activeChanJitter
        self.activeChanProb   = activeChanProb

        self.calibDelay  = calibDelay
        self.calibJitter = calibJitter
        self.calibProb   = calibProb

        self.rocDelay  = rocDelay
        self.rocJitter = rocJitter
        self.rocProb   = rocProb

        self.statsDelay  = statsDelay
        self.statsJitter = statsJitter
        self.statsProb   = statsProb

    def gentablesFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_idq_%s-%d-%d.xml.gz"%(ifo, classifier, start, dur)

    def genFAPFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s-%d-%d.json"%(ifo, classifier, start, dur)

    def genGWFFilenames(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_idq_%s_fap-%d-%d.gwf"%(ifo, classifier, start, dur)
        raise NotImplementedError, "%s_idq_%s_rank-%d-%d.gwf"%(ifo, classifier, start, dur)

    def genTimeseriesFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s_timeseries-%d-%d.png"%(ifo, classifier, start, dur)

    def genActiveChanFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s_chanlist-%d-%d.json"%(ifo, classifier, start, dur)
        raise NotImplementedError, "%s_%s_chanstrip-%d-%d.png"%(ifo, classifier, start, dur)

    def genCalibFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s_calib-%d-%d.json"%(ifo, classifier, start, dur)
        raise NotImplementedError, "%s_%s_calib-%d-%d.png"%(ifo, classifier, start, dur)

    def genROCFilename(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s_ROC-%d-%d.json"%(ifo, classifier, start, dur)
        raise NotImplementedError, "%s_%s_ROC-%d-%d.png"%(ifo, classifier, start, dur)

    def genStatsFilenames(self, instrument, classifier, directory='.'):
        raise NotImplementedError, "%s_%s_calibStats-%d-%d.json"%(ifo, classifier, start, dur)
        raise NotImplementedError, "%s_%s_trainStats-%d-%d.json"%(ifo, classifier, start, dur)

    def drawFAP(self):
        '''
        draw FAP so it is uniform in log(FAP) between self.minFAP and self.maxFAP
        '''
        return np.exp( np.log(self.minFAP) + random.random()*(np.log(self.maxFAP)-np.log(self.minFAP)) )

    def genSchedule(self, directory='.'):
        '''
        generate a schedule for iDQ uploads
        '''
        sched = schedule.Schedule()

        for instrument in self.instruments:
            if random.random() < self.startProb:
                dt = max(0, random.normalvariate(self.startDelay, startJitter))
                message = 'Started Searching for iDQ information within [-, -] at %s'%instrument
                sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
            
                for classifier in self.classifiers:
                    if random.random() < self.startProb:
                        dt = max(0, random.normalvariate(self.startDelay, startJitter))
                        message = 'Started Searching for iDQ information within [-, -] at %s'%instrument
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.tablesProb:
                        dt += max(0, random.normalvariate(self.tablesDelay, self.tablesJitter))
                        message = 'iDQ glitch tables %s:'%instrument
                        filename = self.genTablesFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.fapProb:
                        dt += max(0, random.normalvariate(self.fapDelay, self.fapJitter))
                        fap = self.drawFAP()
                        message = 'minimum glitch-FAP for %s at %s within [-, -] is %.6f'%(classifier, instrument, fap)
                        filename = self.genFAPFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.gwfProb:
                        dt += max(0, random.normalvariate(self.gwfDelay, self.gwfJitter))
                        fapMessage = 'iDQ fap timesereis for %s at %s within [-, -] :'%(classifier, instrument)
                        rnkMessage = 'iDQ glitch-rank frame for %s at %s within [-, -] :'%(classifier, instrument)
                        fapGWF, rnkGWF  = self.genGWFFilenames(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, fapMessage, filename=fapGWF, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, rnkMessage, filename=rnkGWF, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.timeseriesProb:
                        dt += max(0, random.normalvariate(self.timeseriesDelay, self.timeseriesJitter))
                        message = 'iDQ fap and glitch-rank timeseries plot for %s at %s:'%(classifier, instrument)
                        pngFilename = self.genTimeseriesFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if 'ovl' not in classifier: ### warning! this is dangerous hard coding!
                        pass
                    elif random.random() < self.activeChanProb:
                        dt += max(0, random.normalvariate(self.activeChanDelay, self.activeChanJitter))
                        jsonMessage = 'iDQ (possible) active channels for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ channel strip chart for %s at %s'%(classifier, instrument)
                        jsonFilename, pngFilename = self.genActiveChanFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.calibProb:
                        dt += max(0, random.normalvariate(self.calibDelay, self.calibJitter))
                        jsonMessage = 'iDQ calibration sanity check for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ calibration sanity check figure for %s at %s'%(classifier, instrument)
                        jsonFilename, pngFilename = self.genCalibFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.rocProb:
                        dt += max(0, random.normalvariate(self.rocDelay, self.rocJitter))
                        jsonMessage = 'iDQ local ROC curves for %s at %s'%(classifier, instrument)
                        pngMessage = 'iDQ local ROC figure for %s at %s'
                        jsonFilename, pngFilename = self.genROCFilename(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, jsonMessage, filename=jsonFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, pngMessage, filename=pngFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                    if random.random() < self.statsProb:
                        dt += max(0, random.normalvariate(self.statsDelay, self.statsJitter))
                        calibMessage = 'iDQ local calibration vital statistics for %s at %s'
                        trainMessage = 'iDQ local training vital statistics for %s at %s'
                        calibFilename, trainFilename = self.genStatsFilenames(instrument, classifier, directory=directory)
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, calibMessage, filename=calibFilename, gdb_url=self.gdb_url ) )
                        sched.insert( schedule.WriteLog( dt, self.graceDBevent, trainMessage, filename=trainFilename, gdb_url=self.gdb_url ) )
                    else:
                        break

                else: ### we made it all the way through! add finish statement
                    message = 'Finished searching for iDQ information within [-, -] at %s'%(instrument)
                    sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, gdb_url=self.gdb_url ) )

        return sched
