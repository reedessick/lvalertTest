description = "a module which simulates event generation uploads from various pipelines"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import schedule

#-------------------------------------------------

### define parent class

class Pipeline(object):
    '''
    an object which simulates info for uploads to GraceDB as part of event creation
    '''
    pipeline         = 'pipeline'
    allowed_groups   = []
    allowed_searches = []

    def __init__(self, group, graceDBevent, search=None, gdb_url='https://gracedb.ligo.org/api/'):
        self.graceDBevent = graceDBevent
        self.gdb_url      = gdb_url

        if self.allowed_groups: ### if specified, check
            assert group in self.allowed_groups, 'group=%s not allowed for pipeline=%s'%(group, self.pipeline)
        self.group  = group
        if self.allowed_searches: ### if specified, check
            assert search in self.allowed_searches, 'search=%s not allowed for pipeline=%s'%(search, self.pipline)
        self.search = search

    def genFiles(self):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter
        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        raise NotImplementedError

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
        return firstFile, [(dt, message, otherFile1), (dt, message, otherFile2), ...]
        '''
        raise NotImplementedError

class OmicronLIB(Pipeline):
    '''
    a Pipeline for oLIB
    '''
    pipeline = 'lib'
    allowed_groups   = ['burst', 'test']
    allowed_searches = ['allsky', None]

    def genFiles(self):
        '''
        write the files needed to create this event. Also return any files that are automatically uploaded shortly thereafter
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
