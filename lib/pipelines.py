description = "a module which simulates event generation uploads from various pipelines"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import json

import schedule

#-------------------------------------------------

### define useful variables
letters = "a b c d e f g h i j k l m n o p q r s t u v w x y z".split()

#-------------------------------------------------

### define parent class

class Pipeline(object):
    '''
    an object which simulates info for uploads to GraceDB as part of event creation
    '''
    pipeline         = 'pipeline'
    allowed_groups   = []
    allowed_searches = []

    def __init__(self, gps, far, instruments, group, graceDBevent, search=None, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        if self.allowed_groups: ### if specified, check
            assert group in self.allowed_groups, 'group=%s not allowed for pipeline=%s'%(group, self.pipeline)
        self.group  = group
        if self.allowed_searches: ### if specified, check
            assert search in self.allowed_searches, 'search=%s not allowed for pipeline=%s'%(search, self.pipline)
        self.search = search

        self.gps = gps
        self.far = far
        self.instruments = instruments

    def genFiles(self, directory='.'):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter
        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        raise NotImplementedError("this is just a parent class. Children should overwrite this method and they're the ones that should actually be used")

    def genSchedule(self):
        '''
        generate schedule for event creation
        '''
        sched = schedule.Schedule()
        firstFile, otherFiles = self.genFiles() ### generate files
        sched.insert( schedule.CreateEvent( 0.0, self.graceDBevent, self.group, self.pipeline, firstFile, search=self.search, gdb_url=self.gdb_url ) )
        for dt, message, filename in otherFiles: ### schedule any ancilliary file uploads
            sched.insert( schedule.WriteLog( dt, self.graceDBevent, message, filename=filename, gdb_url=self.gdb_url ) ) 
        return sched

#------------------------

### define children

#-----------
# Bursts
#-----------
class OmicronLIB(Pipeline):
    '''
    a Pipeline for oLIB
    '''
    pipeline = 'lib'
    allowed_groups   = ['burst', 'test']
    allowed_searches = ['allsky', None]

    def drawBCI(self):
        return max(random.normalvariate( 10, 3 ), 0)

    def drawBSN(self):
        return max(random.normalvariate( 50, 10 ), 0)

    def drawSNRs(self):
        snrs = {}
        network = 0.0
        for ifo in self.instruments:
            snr = max(random.normalvariate( 8, 1 ), 5)
            snrs[ifo] = snr
            network += snr**2
        snrs['Network'] = network**0.5
        return snrs

    def drawHrss(self):
        return {'posterior median' : max(random.normalvariate( 1e-22, 1e-23 ), 1e-25), 
                'posterior mean'   : max(random.normalvariate( 1e-22, 1e-23 ), 1e-25),
               }

    def drawFrequency(self):
        return {'posterior median' : max(random.normalvariate( 300, 25 ), 32),
                'posterior mean'   : max(random.normalvariate( 300, 25 ), 32),
               }

    def drawQuality(self):
        return {'posterior median' : max(random.normalvariate( 8, 1 ), 2),
                'posterior mean'   : max(random.normalvariate( 8, 1 ), 2),
               }

    def genFilename(self, directory="."):
        return os.path.join(directory, "olib_"+"".join(random.choose(letters) for _ in xrange(6))+".json")

    def genFiles(self, directory='.'):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter

        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        d = {0:{'gpstime'       : self.gps,
                'FAR'           : self.far,
                'raw FAR'       : self.far,
                'trials factor' : 1,
                'nevents'       : 1, 
                'likelihood'    : None,
                'BCI'           : self.drawBCI(),
                'BSN'           : self.drawBSN(),
                'instruments'   : ','.join(self.instruments),
                'timeslides'    : {(key,'0.0') for key in self.instruments},
                'Omicron SNR'   : self.drawSNRs(),
                'hrss'          : self.drawHrss(),
                'frequency'     : self.drawFrequency(),
                'quality'       : self.drawQuality(), 
               },
            }
        filename = self.genFilename(directory=directory)
        file_obj = open(filename, 'w')
        file_obj.write( json.dumps(d) )
        file_obj.close()

        return filename, []

class CoherentWaveBurst(Pipeline):
    '''
    a Pipeline for cWB
    '''
    pipeline         = 'cwb'
    allowed_groups   = ['burst', 'test']
    allowed_searches = ['allsky', None]

    def genFiles(self):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter

        NOTE: cWB writes a LOT of info into txt files, most of which is completely ignored. I only reproduce the "key : value" pairs that are used.

        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        raise NotImplementedError

#-----------
# CBC
#-----------
class CBCPipeline(Pipeline):
    '''
    a Pipeline for CBC events
    '''
    pipeline = 'cbcPipeline'
    allowed_groups   = ['gropu', 'test']

    def genFiles(self):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter
        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        raise NotImplementedError

class GSTLAL(CBCPipeline):
    '''
    a Pipeline for GSTLAL
    '''
    pipeline = 'gstlal'
    allowed_searches = ['lowmass', 'highmass', None]

class GSTLALSpiir(CBCPipeline):
    '''
    a Pipeline for GSTLAL-Spirr
    '''
    pipeline = 'gstlal-spiir'
    allowed_searches = ['lowmass', 'highmass', None]

class MBTAOnline(CBCPipeline):
    '''
    a Pipeline for MBTA
    '''
    pipeline = 'mbtaonline'
    allowed_searches = ['lowmass', 'highmass', None]

class PYCBC(CBCPipeline):
    '''
    a Pipeline for pycbc
    '''
    pipeline = 'pycbc'
    allowed_searches = ['allsky', None]
