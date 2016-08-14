#!/usr/bin/python
usage = "sanityCheck_FakeDb.py [--options]"
description = "provides unit tests and basic checks of FakeDb functionality"
author = "reed.essick@ligo.org"

#-------------------------------------------------

import os

import random

from ligoTest.gracedb.rest import FakeDb

from optparse import OptionParser

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option('-v', '--verbose', default=False, action='store_true')

parser.add_option('-N', '--Nevents', default=5, type='int', help='the number of events we create while testing')
parser.add_option('', '--group', default='cbc', type='string')
parser.add_option('', '--pipeline', default='gstlal', type='string')
parser.add_option('', '--search', default=None, type='string')

parser.add_option('-o', '--output-dir', default='.', type='string')

opts, args = parser.parse_args()

#-------------------------------------------------

labels = "EM_READY PE_READY EM_Throttled EM_Selected EM_Superseded ADVREQ ADVOK ADVNO H1OPS H1OK H1NO L1OPS L1OK L1NO".split()

#-------------------------------------------------

if opts.verbose:
    print "instantiating FakeDb"
gdb = FakeDb(opts.output_dir)

if opts.verbose:
    print "creating events"

tmpfilename = os.path.join(opts.output_dir, "tmpfile.xml")
open(tmpfilename, 'w').close()

graceids = {}
for x in xrange(opts.Nevents):
    if opts.verbose:
        print "    %d / %d : group, pipeline, search = %s, %s, %s"%(x+1, opts.Nevents, opts.group, opts.pipeline, opts.search)

    ### create the event
    response = gdb.createEvent(opts.group, opts.pipeline, tmpfilename, search=opts.search).json()
    graceid = response['uid']
    graceids[graceid] = {'logs'   : [], 
                         'labels' : [], 
                         'files'  : [],
                        }

    if opts.verbose:
        print "      -> %s"%(graceid)

    ### put some stuff into the event
    ### writeLabel
    for label in set( [random.choice(labels) for _ in xrange(5)] ):
        response = gdb.writeLabel( graceid, label ).json()
        graceids[graceid]['logs'].append( response )
        graceids[graceid]['labels'].append( label )
    
    ### removeLabel
#    gdb.removeLabel( graceid, graceids[graceid]['labels'].pop(random.choice(range(len(graceids[graceid]['labels'])))) )

    ### writeLog
    for num in xrange(5):
        message = 'message number : %d'%(num+1)
        response = gdb.writeLog( graceid, message='message number : %d'%(num+1) ).json()
        graceids[graceid]['logs'].append( response )

    for num in xrange(5):
        message  = "message with file number : %d"%(num+1)
        filename = os.path.join( opts.output_dir, "%s-%d.txt"%(graceid, num+1) )
        open(filename, 'w').close()

        response = gdb.writeLog( graceid, message=message, filename=filename ).json()

        graceids[graceid]['logs'].append( response )
        graceids[graceid]['files'].append( filename )

    ### writeFile
    for num in xrange(5):
        filename = os.path.join( opts.output_dir, "%s_%d.txt"%(graceid, num+1) )
        open(filename, 'w').close()

        response = gdb.writeFile( graceid, filename=filename ).json()

        graceids[graceid]['logs'].append( response )
        graceids[graceid]['files'].append( filename )

for graceid in sorted(graceids.keys()):

    ### query information about this event
    ### event
    response = gdb.event( graceid ).json()
    raise NotImplementedError('set up assertion statements to make sure queries return what we put in them')

    ### logs
    response = gdb.logs( graceid ).json()
    raise NotImplementedError('set up assertion statements to make sure queries return what we put in them')

    ### labels
    response = gdb.labels( graceid ).json()
    raise NotImplementedError('set up assertion statements to make sure queries return what we put in them')

    ### files
    response = gdb.files( graceid ).json()
    raise NotImplementedError('set up assertion statements to make sure queries return what we put in them')

### query things about groups of events
### events
raise NotImplementedError('need to set up queries over multiple events?')
