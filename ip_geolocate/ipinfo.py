#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch, requests, json
import time, locale, datetime
import socket
import dns, dns.name, dns.query, dns.resolver, dns.exception
import list_data
import data


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1
DEBUG4 = 0


###################
## Variables
###################
username = 'cuhk_cse_02'
projname = 'cdn_selection'
taskname = 'ip_geolocate'
progname = 'ipinfo'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

ips_dir    = '../../data/ip_search/ips/'
geo_db_dir = '../../data/' + taskname + '/database/'

ip_dict_filename   = 'ips.data'
ipinfo_db_filename = 'ipinfo_db.data'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 60

located_ips = {}


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


def store_output_files():
  data.store_data(geo_db_dir + ipinfo_db_filename, located_ips)


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  if IF_DATA_READ:
    store_output_files()
  os.system("touch %s" % (KILLED_IND_FILE))
  os.system("rm %s" % (RUNNING_IND_FILE))
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


###################
## DEBUG
# exit();
###################


###################
## Main
###################
force_utf8_hack()
os.system("rm %s" % (DONE_IND_FILE))
os.system("rm %s" % (KILLED_IND_FILE))
os.system("touch %s" % (RUNNING_IND_FILE))

###################
## Read IP to Geolocation Database
###################
if DEBUG2: print "Read IP to Geolocation Database"

located_ips = data.load_data(geo_db_dir + ipinfo_db_filename)
IF_DATA_READ = 1

if DEBUG3: print "  #records=%d" % (len(located_ips))
if DEBUG4:
  for ip in located_ips:
    # print located_ips[ip]
    if 'loc' in located_ips[ip]:
      print "  %s, %s" % (ip, located_ips[ip]['loc'])
    else:
      print "  %s\n    " % (ip)
      print located_ips[ip]

# exit()

###################
## read ips
###################
if DEBUG2: print "Read IPs"

if os.path.exists(PARAM_FILE):
  remain_ips = set(list_data.load_data(PARAM_FILE))
else:
  ips = data.load_data(ips_dir + ip_dict_filename)
  remain_ips = set([])
  for cname in ips:
    for dns in ips[cname]:
      for ip in ips[cname][dns]:
        if ip not in located_ips:
          remain_ips.add(ip)

remain_ips = list(remain_ips)
if DEBUG3: print "  #remainning ips: %d" % (len(remain_ips))


###################
## Locate IP
###################
if DEBUG2: print "Locate IP"


cnt = 0;
for ip in remain_ips:
  try:
    r = requests.get('http://ipinfo.io/' + ip)
    located_ips[ip] = r.json()

  except Exception as e:
    print "  [1] %s -- type: %s, msg: %s" % (ip, type(e), e)
    continue


  if DEBUG3:
    if 'loc' in located_ips[ip]:
      print "%d: %s, %s" % (cnt, ip, located_ips[ip]['loc'])
    else:
      print "%d: %s\n    " % (cnt, ip)
      print located_ips[ip]

  cnt += 1
  if cnt % 100 == 0:
    store_output_files()


store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))

