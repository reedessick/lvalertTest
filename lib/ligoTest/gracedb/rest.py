description = "a module that dummies-up GraceDb interface for testing purposes"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import glob
import shutil

import pickle
import json

#-------------------------------------------------

class FakeTTPResponse():
    """
    a "fake" httpResponse that provides some basic functionality
    """

    def __init__(self, data):
        self.data = json.dumps( data )

    def read(self):
        ans = self.data
        self.data = ''
        return ans

    def json(self):
        return json.loads( self.read() )

#-------------------------------------------------

class FakeDb():
    """
    a "fake" GraceDb that provides some basic functionality managed through the local filesystem.

    NOTE: we do NOT support the full functionality of GraceDb
    """
    __group2letter__ = {'cbc'   : 'G', 
                        'burst' : 'G',
                        'test'  : 'T',
                        'mdc'   : 'M',
                       }

    def __init__(self, directory='.'):
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.home = directory
        self.lvalert = os.path.join(directory, 'lvalert.out') ### file into which we write lvalert messages

    def sendlvalert(self, message ):
        file_obj = open(self.lvalert, 'w')
        print >> file_obj, message
        file_obj.close()

    ### generic utils and data management ###

    def __genGraceID__(self, group):
        '''
        looks up the known GraceIDs within the directory.
        returns the biggest one +1
        if none exist, starts at 000000
        '''
        existing = [int(os.path.basename(_.strip('/'))[1:]) for _ in glob.glob("%s/*/"%self.home)]
        if existing:
            ind = max(existing)+1
        else:
            ind = 0

        return "%s%06d"%(self.__group2letter__[group], ind)
            
    def __directory__(self, graceid):
        '''
        checks to see whether this graceid exists
        '''
        return os.path.join(self.home, graceid)

    def __topLevelPath__(self, graceid):
        return os.path.join(self.home, graceid, 'toplevel.pkl')

    def __filesPath__(self, graceid):
        return os.path.join(self.home, graceid, 'files.pkl')

    def __labelsPath__(self, graceid):
        return os.path.join(self.home, graceid, 'labels.pkl')

    def __logsPath__(self, graceid):
        return os.path.join(self.home, graceid, 'logs.pkl')

    def __append__(self, stuff, path):
        '''append to pkl file'''
        ans = self.__extract__(path)
        ans.append(stuff)
        self.__write__(ans, path)

        return len(ans)-1

    def __write__(self, stuff, path):
        '''write stuff into pkl file'''
        file_obj = open(path, 'w')
        pickle.dump(stuff, file_obj)
        file_obj.close()

    def __extract__(self, path):
        '''read from pkl file'''
        file_obj = open(path, 'r')
        ans = pickle.load(file_obj)
        file_obj.close()

        return ans

    def __createDirectory__(self, graceid):
        '''
        generate local data structure for this graceid
        '''
        d = self.__directory__(graceid)
        if os.path.exists(d):
            raise ValueError('graceid=%s already exists!'%graceid)
        else:
            os.makedirs(d) ### make directory
            ### touch a bunch of files to make sure they exist
            paths = [self.__filesPath__(graceid), 
                     self.__labelsPath__(graceid), 
                     self.__logsPath__(graceid),
                    ]
            for path in paths:
                file_obj = open(path, 'w')
                pickle.dump([], file_obj)
                file_obj.close()

    ### insertion ###

    def __writeTopLevel__(self, graceid, group, pipeline, search=None, data={}):
        self.__write__({'uid':graceid, 'group':group, 'pipeline':pipeline, 'search':search, 'data':data}, self.__topLevelPath__(graceid))

    def __copyFile__(self, graceid, filename, ind=None):
        newFilename = os.path.join(self.home, graceid, os.path.basename(filename))
        shutil.copyfile(filename, newFilename)
        self.__append__( (newFilename, ind), self.__filesPath__(graceid) )

        return newFilename

    def createEvent(self, group, pipeline, filename, search=None, filecontents=None, **kwargs):
        group    = group.lower()
        pipeline = pipeline.lower()
        if search: 
            search = search.lower()

        graceid = self.__genGraceID__(group) ### generate the graceid
        self.__createDirectory__(graceid) ### create local directory

        ### write top level data
        self.__writeTopLevel__( graceid, group, pipeline, search=search )
        ans = self.event( graceid ) 
        self.sendlvalert( ans.data )

        ### write filename to local
        self.sendlvalert( self.writeLog( graceid, 'initial data', filename=filename ).data ) ### sends alert about log message

        return ans

    ### annotation ###

    def writeLog(self, graceid, message, filename=None, filecontents=None, tagname=None, displayName=None):

        jsonD = {'message':message, filename:filename, tagname:tagname}

        ind = self.__append__(jsonD, self.__logsPath__(graceid))
        if filename:
            self.__copyFile__( graceid, filename, ind=ind)

        ans = FakeTTPResponse( jsonD )
        self.sendlvalert( ans.data )
        return ans
       
    def writeFile(self, graceid, filename, filecontents=None):
        return self.writeLog( graceid, '', filename=filename, filecontents=filecontents)

    def writeLabel(self, graceid, label):

        jsonD = {'message':'applying label: %s'%label, 'filename':None, 'tagname':None}

        ind = self.__append__(jsonD, self.__logsPath__(graceid))
        self.__append__( (label, ind), self.__labelsPath__(graceid) )

        ans = FakeTTPResponse( jsonD )
        self.sendlvalert( ans.data )
        return ans

    def removeLabel(self, graceid, label):
        raise NotImplementedError('this is not implemented in the real GraceDb, so we do not implement it here. At least, not yet.')

    ### queries ###

    def events(self, query=None, orderby=None, count=None, columns=None):
        raise NotImplementedError('not sure how to support query logic easily...')

    def event(self, graceid):
        return FakeTTPResponse( self.__extract__( self.__topLevelPath__(graceid) ) )

    def logs(self, graceid):
        return FakeTTPResponse( self.__extract__( self.__logsPath__(graceid) ) )

    def labels(self, graceid, label=''):
        return FakeTTPResponse( self.__extract__( self.__labelsPath__(graceid) ) )

    def files(self, graceid, filename=None, raw=False):
        return FakeTTPResponse( self.__extract__( self.__filesPath__(graceid) ) )
