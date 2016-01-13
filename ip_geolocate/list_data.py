#import json, urllib, urllib2
import sys, os, time
import gzip

# FILE_FRIENDSHIP = "../DATA/USER_FRIENDSHIP"
# FILE_START_POOL = "../DATA/LOC_START_POOL"
# FILE_LOC_LANG = '../DATA/LOC_LANGUAGE'
# FILE_USER_LANG = '../DATA/USER_LANGUAGE'


def load_data(filename):
  list_data = []
  try:
    if filename.endswith('.gz'):
      f = gzip.open(filename, 'rb')
    else:
      f = open(filename, 'r')
    
    print "  load data: " + filename
    tmp = f.read()
    list_data = tmp.split('\n')
    # print "    "+"\n    ".join(list_data)
    f.close()
    
  except IOError:
    # print IOError 
    print "  file not exist: " + filename 
  except:
    print "  cannot load " + filename
    pass    

  return list_data


def store_data(filename, list_data):
  try:
    print '  store data: ' + filename
    f = open(filename, 'w+')
    f.write("\n".join(sorted(list_data)))
    f.close()
  except Exception as e:
    print "  list_data -- cannot store it! type: %s, msg: %s" % (type(e), e)
    pass

