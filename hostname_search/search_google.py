#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch
import time, locale, datetime
from urlparse import urlparse
from google import search, get_page
from list_data import *


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1
DONE_IND_FILE    = 'tmp.search_google.done'
KILLED_IND_FILE  = 'tmp.search_google.killed'
RUNNING_IND_FILE = 'tmp.search_google.running'
PARAM_FILE       = 'tmp.search_google.param.txt'


def force_utf8_hack():
  reload(sys)
  sys.setdefaultencoding('utf-8')
  for attr in dir(locale):
    if attr[0:3] != 'LC_':
      continue
    aref = getattr(locale, attr)
    locale.setlocale(aref, '')
    (lang, enc) = locale.getlocale(aref)
    if lang != None:
      try:
        locale.setlocale(aref, (lang, 'UTF-8'))
      except:
        os.environ[attr] = lang + '.UTF-8'


def get_hostname(url):
  ret = urlparse(url)
  return ret.hostname


def read_hostname_files(path, prefix):
  hostnames = set()
  for fn in os.listdir(path):
    # print "  "+path+fn
    if fnmatch.fnmatch(fn, prefix+'*'):
      # print "  "+path+fn
      hostnames |= set(load_data(path+fn))
      
  return hostnames


def store_output_files():
  store_data(input_dir + hostname_filename, list(hostnames))
  store_data(input_dir + done_filename, done_keywords)


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  store_output_files()
  os.system("touch %s" % (KILLED_IND_FILE))
  os.system("rm %s" % (RUNNING_IND_FILE))
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


###########################
## DEBUG
# exit()
###########################


###################
## Main
###################
force_utf8_hack()
os.system("rm %s" % (DONE_IND_FILE))
os.system("rm %s" % (KILLED_IND_FILE))
os.system("touch %s" % (RUNNING_IND_FILE))


###################
## Variables
###################
input_dir = '../../data/hostname_search/hostname/'
keyword_filename = 'keyword_list.txt'
done_filename = 'keyword_list_done.txt'
hostname_prefix = 'hostnames'
done_keywords = []
hostnames = set()


###################
## Parameters
###################
# if len(sys.argv) < 2: std = 0
# else: std = int(sys.argv[1])
# if len(sys.argv) < 3: end = 100000
# else: end = int(sys.argv[2])

# if DEBUG3: print 'Parameters: std=%d, end=%d' % (std, end)
# if std > end:
#   print "EXIT: std index=%d > end index=%d" % (std, end)
#   exit()


###################
## read keyword list
###################
if DEBUG2: print "Read Keywords"

# keywords = load_data(input_dir + keyword_filename)
keywords = load_data(PARAM_FILE)
if len(keywords) == 0:
  if DEBUG3: print "  no keywords"
  exit()

hostname_filename = "%s.txt" % (hostname_prefix)
done_keywords = load_data(input_dir + done_filename)
remain_keywords = sorted(list(set(keywords) - set(done_keywords)))
########
## DEBUG
# if '北京' in keywords:
#   print 'yes'
# else:
#   print 'no'
########


###################
## read existing hostnames
###################
if DEBUG2: print "Read Existing Hostnames"

hostnames = read_hostname_files(input_dir, hostname_prefix)
if DEBUG3: print "    "+"\n    ".join(list(hostnames))


###################
## search hostnames
###################
if DEBUG2: print "Search Hostnames"

key_cnt = 0
for keyword in remain_keywords:
  key_cnt = key_cnt + 1
  if DEBUG3: print '  '+str(key_cnt)+': '+keyword

  cnt = 0
  try:
    for url in search(keyword, stop=5000, pause=60.0):
      cnt = cnt + 1
      hostname = get_hostname(url)

      ## don't save IP address -- we need hostname
      m = re.match("(\d+\.\d+\.\d+\.\d+)", hostname)
      if m is not None:
        continue
      
      if DEBUG3: print('  %d.%d: %s\n    %s' % (key_cnt, cnt, url, hostname))
      hostnames.add(hostname)

      if cnt % 100 == 0:
        store_output_files()
  except:
    print "  search exception! exit!"
    os.system("touch %s" % (KILLED_IND_FILE))
    os.system("rm %s" % (RUNNING_IND_FILE))
    store_output_files()
    exit()


  done_keywords.append(keyword)
  store_output_files()


###################
## store data
###################
if DEBUG2: print "Store Data"

# print "  "+"\n  ".join(list(hostnames))+"\n"
store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))
