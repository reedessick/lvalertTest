#!/usr/bin/python
usage = "sanityCheck_FakeDb.py [--options]"
description = "provides unit tests and basic checks of FakeDb functionality"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import random

from ligoTest.gracedb.rest import FakeDb

import pipelines
import schedule

from lal.gpstime import tconvert

from optparse import OptionParser

#-------------------------------------------------

def genRandStr():
    alpha = 'A B C D E F G H I J K L M N O P Q R S TU V W Y X Z'.split()
    return "".join(random.choice(alpha) for _ in xrange(6))

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option('-v', '--verbose', default=False, action='store_true')
parser.add_option('-V', '--Verbose', default=False, action='store_true')

parser.add_option('-N', '--Nevents', default=5, type='int', help='the number of events we create while testing')
parser.add_option('', '--group', default='cbc', type='string')
parser.add_option('', '--pipeline', default='gstlal', type='string')
parser.add_option('', '--search', default=None, type='string')

parser.add_option('-f', '--fakeDB-dir', default='./fakeDB', type='string')

parser.add_option('-o', '--output-dir', default='.', type='string')

opts, args = parser.parse_args()

opts.verbose = opts.verbose or opts.Verbose

if not os.path.exists(opts.fakeDB_dir):
    os.makedirs(opts.fakeDB_dir)
if not os.path.exists(opts.output_dir):
    os.makedirs(opts.output_dir)

#-------------------------------------------------

labels = "EM_READY PE_READY EM_Throttled EM_Selected EM_Superseded ADVREQ ADVOK ADVNO H1OPS H1OK H1NO L1OPS L1OK L1NO".split()

#-------------------------------------------------

if opts.verbose:
    print "instantiating FakeDb"
gdb = FakeDb(opts.fakeDB_dir)

if opts.verbose:
    print "creating events"

graceids = {}
for x in xrange(opts.Nevents):
    if opts.verbose:
        print "    %d / %d : group, pipeline, search = %s, %s, %s"%(x+1, opts.Nevents, opts.group, opts.pipeline, opts.search)

    ### create the event
    randStr = genRandStr()
    gDBevent = schedule.GraceDBEvent()
    pipeObj = pipelines.initPipeline( float(tconvert('now')), 
                                      1e-9, 
                                      ['H1','L1'], 
                                      opts.group, 
                                      opts.pipeline, 
                                      gDBevent, 
                                      search=opts.search, 
                                      gdb_url=opts.fakeDB_dir
                                    )

    agenda = pipeObj.genSchedule(directory=opts.output_dir)

    ### put some stuff into the event

    ### writeLabel
    for label in set( [random.choice(labels) for _ in xrange(5)] ):
        agenda.insert( schedule.WriteLabel( 100, gDBevent, label, gdb_url=opts.fakeDB_dir ) )
    
    ### removeLabel
#    agenda.insert( schedule.RemoveLabel( 150, gDBevent, label, gdb_url=opts.fakeDB_dir ) )
    #                                                    ^this should be defined in the previous loop!

    ### writeLog
    for num in xrange(5):
        message = 'message number : %d'%(num+1)
        agenda.insert( schedule.WriteLog( 200, gDBevent, message, gdb_url=opts.fakeDB_dir ) )

    for num in xrange(5):
        message  = "message with file number : %d"%(num+1)
        filename = os.path.join( opts.output_dir, "%s-%d.txt"%(randStr, num+1) )
        open(filename, 'w').close()
        agenda.insert( schedule.WriteLog( 300, gDBevent, message, filename=filename, gdb_url=opts.fakeDB_dir ) )

    ### writeFile
    for num in xrange(num, num+5):
        filename = os.path.join( opts.output_dir, "%s_%d.txt"%(randStr, num+1) )
        open(filename, 'w').close()
        agenda.insert( schedule.WriteFile( 400, gDBevent, filename, gdb_url=opts.fakeDB_dir ) )

    ### iterate and do things
    for action in agenda:
        if opts.Verbose:
            print " ", action

        response = action.execute() ### do the thing

        if isinstance(action, schedule.CreateEvent): ### this should be the first action so we should be safe defining things herein...
            graceid = gDBevent.get_graceid()
            graceids[graceid] = {'event'  : {'gpstime'  : pipeObj.gps,
                                             'far'      : pipeObj.far,
                                             'group'    : pipeObj.group.lower(),
                                             'pipeline' : pipeObj.pipeline.lower(),
                                             'search'   : pipeObj.search.lower() if pipeObj.search else pipeObj.search,
                                             'gDBevent' : pipeObj.graceDBevent,
                                            },
                                 'logs'   : [],
                                 'labels' : [],
                                 'files'  : [action.filename],
                                }

        elif isinstance(action, schedule.WriteLabel):
            graceids[graceid]['labels'].append( action.label )

        elif isinstance(action, schedule.RemoveLabel):
            raise NotImplementedError('we do not currently support RemoveLabel actions...')

        elif isinstance(action, schedule.WriteLog):
            graceids[graceid]['logs'].append( response.json() )
            if action.filename:
                graceids[graceid]['files'].append( action.filename )

        elif isinstance(action, schedule.WriteFile):
            graceids[graceid]['files'].append( action.filename )

        else:
            print "action not understood!\n%s"%action


#-------------------------------------------------

if opts.verbose:
    print "checking FakeDb via queries"

for graceid in sorted(graceids.keys()):

    if opts.verbose:
        print "investigating : %s"%graceid

    metadata = graceids[graceid]

    ### query information about this event

    #--------------------

    ### event
    if opts.verbose:
        print "\nFakeDb.event()"
    response = gdb.event( graceid ).json()
    if opts.Verbose:
        for key, value in response.items():
            print "    ", key, "\t", value

    metadatum = metadata['event']

    assert response['graceid'] == graceid,                             'graceid is wrong : %s vs %s'%(response['graceid'], graceid)
    assert graceid             == metadatum['gDBevent'].get_graceid(), 'graceDBevent->graceid is wrong : %s vs %s'%(graceid, metadatum['gDBevent'].get_graceid())

    assert response['gpstime']  == metadatum['gpstime'],  'gps is wrong : %.6f vs %.6f'%(response['gpstime'], metadatum['gpstime'])
    assert response['far']      == metadatum['far'],      'far is wrong : %.6e vs %.6e'%(response['far'], metadatum['far'])
    assert response['group']    == metadatum['group'],    'group is wrong : %s vs %s'%(response['group'], metadatum['group'])
    assert response['pipeline'] == metadatum['pipeline'], 'pipeline is wrong : %s vs %s'%(response['pipeline'], metadatum['pipeline'])
    assert response['search']   == metadatum['search'],   'search is wrong : %s vs %s'%(response['search'], metadatum['search'])

    if opts.verbose:
        print "passed all checks!"

    #--------------------

    ### labels
    if opts.verbose:
        print "\nFakeDb.labels()"
    response = gdb.labels( graceid ).json()
    if opts.Verbose:
        for key, value in response.items():
            print "    ", key
            for val in value:
                print "    \t", value

    metadatum = metadata['labels']
    labels = [label['name'] for label in response['labels']]
    if opts.Verbose:
        print "  ensuring all labels associated with %s were actually applied"%graceid

    for label in labels:
        assert label in metadatum, 'label=%s present but not uploaded'%(label)

    if opts.Verbose:
        print "  passed all checks!"
        print "  ensuring all applied labels are actually present"

    for label in metadatum:
        assert label in labels,    'label=%s uploaded but not present'%(label)

    if opts.Verbose:
        print "  passed all checks!"

    if opts.verbose:
        print "passed all checks!"

    #--------------------

    ### files
    if opts.verbose:
        print "\nFakeDb.files()"
    response = gdb.files( graceid ).json()
    if opts.Verbose:
        for key, value in response.items():
            print "    ", key, "\t", value

    metadatum = [os.path.basename(filename) for filename in metadata['files']]
    files = response.keys()
    if opts.Verbose:
        print "  ensuring all files associated with %s were actually uploaded"%graceid

    for filename in files:
        assert filename in metadatum, "file=%s present but not uploaded"%(filename)

    if opts.Verbose:
        print "  passed all checks!"
        print "  ensuring all uploaded files are atually present"

    for filename in metadatum:
        assert filename in files,     "file=%s uploaded but not present"%(filename)

    if opts.Verbose:
        print "  passed all checks!"

    if opts.verbose:
        print "passed all checks!"

    #--------------------

    ### logs
    if opts.verbose:
        print "\nFakeDb.logs()"
    response = gdb.logs( graceid ).json()
    if opts.Verbose:
        for key, value in response.items():
            if key == 'log':
                print "    ", key
                for val in value:
                    print "    \t", val
            else:
                print "    ", key, "\t", value

    logs = response['log']

    # check that labelling produces logs
    if opts.Verbose:
        print "  ensuring labels generated logs"

    for label in metadata['labels']:
        for log in logs:
            if log['comment'] == 'applying label : %s'%label:
                break
        else:
            assert False, 'label=%s did not produce a log message'%label

    if opts.Verbose:
        print "  passed all checks!"

    # check that files produced logs
    if opts.Verbose:
        print "  ensuring files are attached to logs"

    for filename in [os.path.basename(filename) for filename in metadata['files']]:
        for log in logs:
            if log['filename'] == filename:
                break
        else:
            assert False, 'file=%s was uploaded but produced no log'%filename

    if opts.Verbose:
        print "  passed all checks!"

    # check that logging produced logs
    if opts.Verbose:
        print "  ensuring all log messages were recorded"

    for log in metadata['logs']:
        comment = log['comment']
        for log in logs:
            if log['comment'] == comment:
                break
        else:
            assert False, 'log with comment=%s was uploaded but not recorded'%comment

    if opts.Verbose:
        print "  passed all checks!"

    if opts.verbose:
        print "passed all checks!"

#-------------------------------------------------

### query things about groups of events
### events
#raise NotImplementedError('need to set up queries over multiple events?')
