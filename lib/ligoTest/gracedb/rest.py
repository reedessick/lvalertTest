description = "a module that dummies-up GraceDb interface for testing purposes"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import glob
import shutil

import getpass

import pickle
import json

import time

import numpy as np

from glue.ligolw import utils as ligolw_utils
from glue.ligolw import ligolw
from glue.ligolw import table
from glue.ligolw import lsctables

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

    def __newfilename__(self, graceid, filename):
        return os.path.join(self.home, graceid, os.path.basename(filename))

    def __copyFile__(self, graceid, filename):
        newFilename = self.__newfilename__(graceid, filename)
        shutil.copyfile(filename, newFilename)
        self.__append__( newFilename, self.__filesPath__(graceid) )

    ### insertion ###

    def __createEvent__(self, graceid, group, pipeline, filename, search=None):
        jsonD = {'graceid':graceid,
                 'group'  :group,
                 'pipeline':pipeline,
                 'search':search,
                 'created':time.time(),
                 'submitter':getpass.getuser()+'@ligo.org',
                 'links': {'neighbors':'',
                           'files':self.__filesPath__(graceid),
                           'log':self.__logsPath__(graceid),
                           'tags':'',
                           'self':self.__directory__(graceid),
                           'labels':self.__labelsPath__(graceid),
                           'filemeta':self.__topLevelPath__(graceid),
                           'emobservations':'',
                          },
                }

        ### extract these by parsing filename!
        jsonD.update( self.__file2extraattributes__(pipeline, filename) )
         
        ### write top level data to file 
        self.__write__( jsonD, self.__topLevelPath__(graceid) )

        lvalert = {"alert_type": "new",
                   "description": "",
                   "file": self.__newfilename__(graceid, filename),
                   "object": jsonD,
                   "uid": graceid,
                  }

        return jsonD, lvalert

    def __file2extraattributes__(self, pipeline, filename):
        if pipeline == 'cwb':
            file_obj = open(filename, 'r')
            ans = {'extra_attributes':{
                                      },
                  }

            far = np.infty

            readme = False
            for line in file_obj:
                try:
                    key, val = line.split(':')
                    key = key.strip()
                    if key == "likelihood":
                        ans['likelihood'] = float(val.strip())
                    elif key == 'time':
                        ans['gpstime'] = float(val.split()[0].strip())
                    elif key == 'ifo':
                        ans['instruments'] = ",".join(val.split())
                except:
                    if readme:
                        fields = [float(_) for _ in line.strip().split()]
                        ans['far'] = fields[1]
                        break
                    
                    readme = "significance based on " in line ### next line is a FAR statement
                        
            return ans

        elif pipeline == 'lib':
            file_obj = open(filename, 'r')
            a = json.loads( file_obj.read() )["0"]
            file_obj.close()

            ans = {'gpstime'    : a['gpstime'],
                   'FAR'        : a['FAR'],
                   'instruments': a['instruments'],
                   'likelihood' : a['likelihood'],
                   'nevents'    : a['nevents'],
                   'extra_attributes' : {'raw FAR'    : a['raw FAR'],
                                         'BCI'        : a['BCI'], 
                                         'BSN'        : a['BSN'],
                                         'Omicron SNR': a['Omicron SNR'],
                                         'timeslides' : a['timeslides'], 
                                         'hrss'       : a['hrss'],
                                         'frequency'  : a['frequency'],
                                         'quality'    : a['quality'],
                                        },
                  }

            return ans

        elif pipeline in ['gstlal', 'gstlal-spiir', 'mbtaonline', 'pycbc']:
            xmldoc = ligolw_utils.load_filename(filename, contenthandler=lsctables.use_in(ligolw.LIGOLWContentHandler))

            ### extract table
            coinc = table.get_table(xmldoc, lsctables.CoincInspiralTable.tableName)

            ### fill in ans with
            for row in coinc:
                ans = {'far' : row.false_alarm_rate,
                       'instruments' : row.ifos,
                       'gpstime'     : row.end_time + 1e-9*row.end_time_ns,
                       'extra_attributes': {
                                           },
                      }

            return ans

        else:
            raise ValueError('pipeline=%s not understood'%pipeline)


    def createEvent(self, group, pipeline, filename, search=None, filecontents=None, **kwargs):
        group    = group.lower()
        pipeline = pipeline.lower()
        if search: 
            search = search.lower()

        graceid = self.__genGraceID__(group) ### generate the graceid
        self.__createDirectory__(graceid) ### create local directory and all necessary files

        ### write top level data
        jsonD, lvalert = self.__createEvent__( graceid, group, pipeline, filename, search=search )
        self.sendlvalert( lvalert )

        ### write filename to local
        self.writeLog( graceid, 'initial data', filename=filename ) ### sends alert about log message

        return FakeTTPResponse( jsonD )

    ### annotation ###

    def __log__(self, graceid, message, filename=None, tagname=[]):
        username = getpass.getuser()

        if filename:
            shortFilename = os.path.basename(filename)
        else:
            shortFilename = ''

        jsonD = {'comment': message,
                 'created': time.time(),
                 'self': self.__logsPath__(graceid),
                 'file_version': 0,  
                 'filename': shortFilename,
                 'tag_names': tagname,
                 'file': '',
                 'N': 0,  
                 'tags': '',
                 'issuer': {'username': username+'@LIGO.ORG',
                            'display_name': username,
                           },
                }

        ind = self.__append__( jsonD, self.__logsPath__(graceid))
        if filename:
            self.__copyFile__(graceid, filename)

        lvalert = {'uid':graceid, 
                   "alert_type": "update",
                   "description": message,
                   "file": shortFilename,
                   "object": {
                              "N": ind,
                              "comment": message,
                              "created": time.time(),
                              "file": shortFilename,
                              "file_version": 0,
                              "filename": shortFilename,
                              "issuer": {
                                         "display_name": username,
                                         "username": username+"@ligo.org"
                                        },
                              "self": "",
                              "tag_names": tagname,
                              "tags": "",
                             },
                  } 

        return jsonD, lvalert

    def writeLog(self, graceid, message, filename=None, filecontents=None, tagname=[], displayName=None):

        jsonD, lvalert = self.__log__(graceid, message, filename=filename, tagname=tagname )

        self.sendlvalert( lvalert )
        return FakeTTPResponse( jsonD )
 
    def writeFile(self, graceid, filename, filecontents=None):
        return self.writeLog( graceid, '', filename=filename, filecontents=filecontents)

    def __label__(self, graceid, label ):
        jsonD = {'self':self.__labelsPath__(graceid), 
                 'creator':getpass.getuser(), 
                 'name':label, 
                 'created':time.time(),
                }
        lvalert = {'uid':graceid, 
                   'alert_type':'label', 
                   'description':label, 
                   'file':'',
                  }

        self.writeLog( graceid, 'applying label : %s'%label )
        self.__append__( jsonD, self.__labelsPath__(graceid) )

        return jsonD, lvalert

    def writeLabel(self, graceid, label):
        jsonD, lvalert = self.__label__( graceid, label )

        self.sendlvalert( lvalert )
        return FakeTTPResponse( jsonD )

    def removeLabel(self, graceid, label):
        raise NotImplementedError('this is not implemented in the real GraceDb, so we do not implement it here. At least, not yet.')

    ### queries ###

    def events(self, query=None, orderby=None, count=None, columns=None):
        raise NotImplementedError('not sure how to support query logic easily...')

    def event(self, graceid):
        topLevel = self.__extract__( self.__topLevelPath__(graceid) )
        topLevel.update( {'labels':dict( (label['name'], label['self']) for label in self.__extract__( self.__labelsPath__(graceid) ) )} )

        return FakeTTPResponse( topLevel )

    def logs(self, graceid):
        logs = self.__extract__( self.__logsPath__(graceid) )
        return FakeTTPResponse( {'numRows':len(logs),
                                 'start':0,
                                 'log': logs,
                                 'links':{'self':self.__logPath__(graceid),
                                          'first':self.__logPath__(graceid),
                                          'last':self.__logPath__(graceid), 
                                         },
                                }
                              )

    def labels(self, graceid, label=''):
        return FakeTTPResponse( {'labels':self.__extract__( self.__labelsPath__(graceid) ),
                                 'links': [{'self':self.__labelsPath__(graceid),
                                           'event':self.__directory__(graceid),
                                           }
                                          ],
                                }
                              )

    def files(self, graceid, filename=None, raw=False):
        return FakeTTPResponse( dict( (os.path.basename(filename), filename) for filename in self.__extract__( self.__filesPath__(graceid) ) ) )
