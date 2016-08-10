#!/usr/bin/python
usage = "sanityCheck_FakeDb.py [--options]"
description = "provides unit tests and basic checks of FakeDb functionality"
author = "reed.essick@ligo.org"

#-------------------------------------------------

from ligoTest.gracedb.rest import FakeDb

from optparse import OptionParser

#-------------------------------------------------

parser = OptionParser(usage=usage, description=description)

parser.add_option('-v', '--verbose', default=False, action='store_true')

opts, args = parser.parse_args()

#-------------------------------------------------

'''
we should check the following:

    event creation and creation of local directory structure

    querying events:
        logs
        files
        labels

    annotating events:
        writeLabel
        removeLabel <-- just raise NotImplementedError until this is a reality in GraceDB itself
        writeLog
        writeFile
    
there is other functionality that we could mimic, but this should suffice for most needs. 
'''
