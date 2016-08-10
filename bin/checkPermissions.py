#!/usr/bin/python
usage       = "checkPermissions.py [--options]"
description = "attempts to create GraceDB events and apply labels in order to confirm permissions for these actions"
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import os
import traceback

import time

import schedule
import pipelines 

from lal.gpstime import tconvert

from optparse import OptionParser

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option('-v', '--verbose', default=False, action="store_true")
parser.add_option('-V', '--Verbose', default=False, action="store_true")

parser.add_option('-g', '--gracedb-url', default='https://gracedb.ligo.org/api/', type='string')

parser.add_option('-o', '--output-dir', default='.', type='string')
parser.add_option('-T', '--test', default=False, action='store_true', help='do not actually perform actions, but just print them to stdout')

opts, args = parser.parse_args()

if not os.path.exists(opts.output_dir):
    os.makedirs(opts.output_dir)

opts.verbose = opts.verbose or opts.Verbose

execute = not opts.test

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

gps = float(tconvert( 'now' ))
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
                action.setExpiration(time.time())
                if opts.Verbose:
                    print action

                if execute:
                    try:
                        action.execute() 
                        successes.append( (group, pipeline, search) )
                    except Exception as exc:
                        failures.append( ((group, pipeline, search), exc) )
if execute:
    print "user is allowed to create the following"
    for group, pipeline, search in successes:
        print "    group, pipeline, search : %s, %s, %s"%(group, pipeline, search)

    print "user is forbidden from creating the following"
    for (group, pipeline, search), exc in failures:
        print "    group, pipeline, search : %s, %s, %s"%(group, pipeline, search)
        if opts.Verbose:
            traceback.print_exc(exc)

#-------------------------------------------------

### check labeling permisions

### ensure we succeeded in creating at least one event
if execute:
    for graceDBevent in graceDBevents:
        try:
            graceDBevent.get_graceid()
            break
        except:
            pass
    else:
        raise ValueError('we must succeed in creating at least one event new event to test labels')

    if opts.verbose:
        print "attempting to label graceid=%d"%graceDBevent.get_graceid()

### known labels
labels  = "INJ DQV EM_READY PE_READY EM_Throttled EM_Selected EM_Superseded".split()
labels += ["ADV%s"%x for x in "REQ OK NO".split()]
labels += ["%s%s"%(ifo, x) for ifo in "H1 L1".split() for x in "OPS OK NO".split()]

successes = []
failures  = []
for label in labels:
    if opts.verbose:
        print "  trying:\n    label : %s"%label

    action = schedule.WriteLabel( 0.0, graceDBevent, label, gdb_url=opts.gracedb_url )
    action.setExpiration(time.time())
    if opts.Verbose:
        print action 

    if execute:
        try:
            action.execute()
            successes.append( label )
        except Exception as exc:
            failures.append( (label, exc) )

if execute:
    print "user can apply the following labels"
    for label in successes:
        print "    "+label

    print "user cannot apply the following labels"
    for label, exc in failures:
        print "    "+label
        if opts.Verbose:
            traceback.print_exc(exc)
