description = "a module which simulates event generation uploads from various pipelines"
author = "reed.essick@ligo.org"

#-------------------------------------------------

'''
define a different class for each pipeline
these should inherit from a single parent with the following features
  attributes:
    name
  methods:
    generateEvent

we can set up common things for CBC events which share the same coinc.xml format, etc
'''
