# lvalertTestI've set up an lvalert node specifically for testing purposes. Here are the pertinents:

    username : gdb_processor
    password : skrite#batz

    resource : lvalertMP-test

    node : lvalertMP-TestNode

I've also written a simple script to ping this node with basic messages

    lvalertMP_test

and a config file with which I can run lvalert_listenMP[simple]

    lvalert_listenMPsimple.ini (points to childConfig.ini)

as well as a config file for a more standard lvalert_listen process to confirm we heard the alerts.

    lvalert_listen.ini (delegates to confirmation.sh)

This should give me a good enough place to start hard-core developing and debugging.

--------------------------------------------------

this appears to be working, at least in principle. We need to do the following, however:

  -- improve logging within multiprocessingChildSimple.py so that everything comes with a big time-stamp and so that I can trace everything
  -- implement a few basic modules for a few basic checks. For example, start with iDQ FAP statements and see if you can get this to work as desired.
  -- implement check-pointing so that the process can be revived "without loss." Checkpointing must actually occur within mpChildSimple.py, but the control arguments should be passed in through lvalert_listenMPsimple (specified in config file)

We also need to come up with a way to specify which "type" of process this is so that we load the appropriate parseAlert function and associated libraries. This is more of an organizational detail than a serious technical impediment.
  -- current solution is an option in childConfig which will tell the script which library to source for parseAlert()

--------------------------------------------------

To Do:

  -- set up poisson event generator
    -- mimic each type of event (ie: pipeline)
  -- set up dummy messages for expected follow-up processes
  -- may want to submit events to an instance of simDB rather than just sending alerts?
