#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, time
import cPickle
import gzip

def load_data(FILE_NAME):
  LOADED_DATA = {}
  try:
    if FILE_NAME.endswith('.gz'):
      FILE = gzip.open(FILE_NAME, 'rb')
    else:
      FILE = open(FILE_NAME, 'r')

    LOADED_DATA = cPickle.load(FILE)
    FILE.close()

    print "  load data: " + FILE_NAME

  except cPickle.PickleError as e:
    print "  cannot load! type=%s, msg=%s" % (type(e), e)
  except IOError:
    print "  file not exist: " + FILE_NAME
  except:
    print "  cannot load: " + FILE_NAME
    pass
  return LOADED_DATA

def store_data(FILE_NAME, DATA_TOSTORE):
  try:
    print '  store data: ' + FILE_NAME
    FILE = open(FILE_NAME, 'w+')
    cPickle.dump(DATA_TOSTORE, FILE)
    FILE.close()
  except Exception as e:
    print "  cannot store it! type: %s, msg: %s" % (type(e), e)
    pass

