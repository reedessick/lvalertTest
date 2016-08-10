#!/usr/bin/python
usage       = "checkPermissions.py [--options]"
description = "attempts to create GraceDB events and apply labels in order to confirm permissions for these actions"
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import schedule

from optparse import OptionParser

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option('-v', '--verbose', default=False, action="store_true")

parser.add_option('-g', '--gracedb-url', default='https://gracedb.ligo.org/api/', type='string')

parser.add_option('-o', '--output-dir', default='.', type='string')

opts, args = parser.parse_args()

#-------------------------------------------------

### check event creation permissions

if opts.verbose:
    print "attempting to create events"

### known combinations of group_pipeline[_search]
combos = {
          'CBC'   : { 
                     'MBTAOnline'   : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'gstlal'       : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'gstlal-spiir' : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'pycbc'        : [None, 'AllSky', 'LowMass', 'HighMass', 'MDC'],
                    },
          'Burst' : { 
                     'CWB'          : [None, 'AllSky'],
                     'LIB'          : [None, 'AllSky'],
                    },
          'Test'  : { 
                     'MBTAOnline'   : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'gstlal'       : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'gstlal-spiir' : [None, 'LowMass', 'HighMass', 'MDC'], 
                     'pycbc'        : [None, 'AllSky', 'LowMass', 'HighMass', 'MDC'],
                     'CWB'          : [None, 'AllSky'],
                     'LIB'          : [None, 'AllSky'],
                    },
         }

gps = tconvert( 'now' )
far = 1e-9
instruments = ["H1", "L1"]

graceDBevents = []
successes = []
failures  = []
for group in combos.keys():
    for pipeline in combos[group].keys():
        for search in combos[group][pipeline]:
            if opts.verbose: 
                print "  trying:\n    group    : %s\n    pipeline : %s\n    search   : %s"%(group, pipeline, search)

            graceDBevent = schedule.GraceDBEvent()
            graceDBevents.append( graceDBevent )

            pipeObj = pipelines.initPipeline( gps, far, instruments, group, pipeline, graceDBevent, search=search, gdb_url=opts.gracedb_url)
            for action in pipeObj.genSchedule(directory=opts.output_dir):
                try:
                    action.execute() 
                    successes.append( (group, pipeline, search) )
                except Exception as exe:
                    failures.append( ((group, pipeline, search), exe) )

raise NotImplementedError("write summary of permissions for event creation here")

#-------------------------------------------------

### check labeling permisions

### ensure we succeeded in creating at least one event
for graceDBevent in graceDBevents:
    try:
        graceDBevent.get_graceid()
        break
    except:
        pass
else:
    raise ValueError('we must succeed in creating at least one event new event to test labels')

if opts.verbose:
    print "attempting to label events"

### known labels
labels  = "INJ DQV EM_READY PE_READY EM_Throttled EM_Selected EM_Superseded".split()
labels += ["ADV%s"%x for x in "REQ OK NO".split()]
labels += ["%s%s"%(ifo, x) for ifo in "H1 L1".split() for x in "OPS OK NO".split()]

successes = []
failures  = []
for label in labels:
    action = schedule.WriteLabel( 0.0, graceDBevent, label, gdb_url=opts.gracedb_url )
    try:
        action.execute()
        successes.append( label )
    except Exception as exe:
        failures.append( (label, exe) )

raise NotImplementedError('write a summary of permissions for labels here')
