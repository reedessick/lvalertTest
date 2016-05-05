#!/usr/bin/python
usage       = "lvalertMP_test [--options] arg"
description = "a script to send test messages through lvalertMP-TestNode for debugging purposes"
author      = "reed.essick@ligo.org"

#-------------------------------------------------

import random
import time

import json

import subprocess as sp

import argparse

#-------------------------------------------------

def random_gid():
    """ generate a fake ID for this event """
    numbers = "0 1 2 3 4 5 6 7 8 9".split()
    return "F"+"".join(random.choice(numbers) for _ in xrange(6))

def start_message( gid ):
    """ generate a fake "start" message for this event """
    return json.dumps( {'uid':gid, 'log':{'comment':'madeUpProcess started'}} )

def finish_message( gid ):
    """ generate a fake "finish" message for this event """
    return json.dumps( {'uid':gid, 'log':{'comment':'madeUpProcess finished'}} )

def error_message( gid ):
    """ generate a fake "error" message for this event """
    return json.dumps( {'uid':gid, 'log':{'comment':'madeUpProcess failed'}} )

def send_message( message, node, username, password, resource, max_attempts=1 ):
    """ broadcast this message thru lvalert """

    tmpfilename = "tmpfile.json"
    tmpfile = open(tmpfilename, "w")
    tmpfile.write( message )
    tmpfile.close()

    cmd = "lvalert_send -a %s -b %s -r %s -n %s -m %d --file %s"%(username, password, resource, node, max_attempts, tmpfilename)
    return sp.Popen(cmd.split()).wait()

#-------------------------------------------------

parser = argparse.ArgumentParser(usage=usage, description=description)

parser.add_argument('command', 
                    choices=['start', 'finish', 'error', 'simulate'],
                    help="send a message that mimics these behaviors for a fictional follow-up process. If command=simulate, we space out a series of messages as if they were coming from a real event."
                   )

### printing options
parser.add_argument('--verbose',
                    default=False, 
                    action='store_true',
                    help="print informative statements"
                   )

### lvalert options
parser.add_argument('--node',
                    default='lvalertMP-TestNode',
                    type=str,
                    help='the node to use for lvalert messages'
                   )

parser.add_argument('--username',
                    default='gdb_processor',
                    type=str,
                    help='the username to use for lvalert messages'
                   )

parser.add_argument('--password',
                    default='skrite#batz',
                    type=str,
                    help='the password to use for lvalert messages'
                   )

parser.add_argument('--resource',
                    default='lvalertMP-test',
                    type=str,
                    help='the resource name to use for lvalert messages'
                   )


### simulation options
parser.add_argument('--graceID',
                    default=None,
                    type=str,
                    help='use this graceID when simulating messages'
                   )

parser.add_argument('--simulate-Ntrials',
                    default=1,
                    type=int,
                    help="repeat simulation this number of times"
                   )
parser.add_argument('--simulate-wait',
                    default=60,
                    type=float,
                    help="the amount of time before we send a either a finish or error message"
                   )
parser.add_argument('--simulate-jitter',
                    default=10,
                    type=float,
                    help='we draw a random jitter from a uniform distribution and shift the time we wait by that amount'
                   )
parser.add_argument('--simulate-errorProbability',
                    default=0.10,
                    type=float,
                    help='the probability that the follow-up processed failed and we need to send an error message'
                   )

args = parser.parse_args()

if args.graceID == None:
    args.graceID = random_gid()

#-------------------------------------------------

if args.command == "start":
    message = start_message( args.graceID )
    if args.verbose:
        print "sending start message for %s : %s"%(args.graceID, message)
    send_message( message, args.node, args.username, args.password, args.resource )

elif args.command == "finish":
    message = finish_message( args.graceID )
    if args.verbose:
        print "sending finish message for %s : %s"%(args.graceID, message)
    send_message( message, args.node, args.username, args.password, args.resource )

elif args.command == "error":
    message = error_message( args.graceID )
    if args.verbose:
        print "sending error message for %s : %s"%(args.graceID, message)
    send_message( message, args.node, args.username, args.password, args.resource )

elif args.command == "simulate":
    for i in xrange(args.simulate_Ntrials):
        ### simulate an entire follow-up process
        wait = args.simulate_wait + random.random()*args.simulate_jitter
        failed = random.random() <= args.simulate_errorProbability

        message = start_message( args.graceID )
        if args.verbose:
            print "sending start message for %s : %s"%(args.graceID, message)
        send_message( message, args.node, args.username, args.password, args.resource )

        if args.verbose:
            print "waiting %.3f seconds"%wait
        time.sleep( wait )
    
        if failed:
            message = error_message( args.graceID )
            if args.verbose:
                print "sending finish message for %s : %s"%(args.graceID, message)
            send_message( message, args.node, args.username, args.password, args.resource )
        else:
            message = finish_message( args.graceID )
            if args.verbose:
                print "sending finish message for %s : %s"%(args.graceID, message)
            send_message( message, args.node, args.username, args.password, args.resource )

if args.verbose:
    print "Done!"
