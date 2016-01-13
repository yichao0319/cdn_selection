#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch, np
import time, locale, datetime, requests
import socket, urllib2, httplib
import dns, dns.name, dns.query, dns.resolver, dns.exception
import list_data
import data
from bs4 import BeautifulSoup
from urlparse import urlparse

import requests.packages.urllib3.util.ssl_
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'


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
taskname = 'hostname_search'
progname = 'parse_links'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

hosts_dir    = '../../data/hostname_search/hostname/'

hostname_filename = 'hostnames.txt'
entrance_filename = 'entrances.txt'
IF_DATA_READ = 0

time_cnt = 0
wait_time = 60

hostnames = set()
url_pool  = {} 


def add_to_hostnames(url):
  parsed_url = urlparse(url)
  hostname = parsed_url.hostname
  if hostname is not None:
    hostnames.add(hostname)
  

def add_to_pool(url):
  parsed_url = urlparse(url)
  hostname = parsed_url.hostname
  if hostname is None: 
    return 0
  
  if hostname in url_pool:
    if url in url_pool[hostname]:
      cnt = 0
    else:
      url_pool[hostname].add(url)
      cnt = 1
  else:
    url_pool[hostname] = set([url])
    cnt = 1
  return cnt


def store_output_files():
  # print "Store!"
  hostnames.update(set(list_data.load_data(hosts_dir + hostname_filename)))
  list_data.store_data(hosts_dir + hostname_filename, list(hostnames))


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
os.system("rm %s" % (DONE_IND_FILE))
os.system("rm %s" % (KILLED_IND_FILE))
os.system("touch %s" % (RUNNING_IND_FILE))


###################
## Read Existing Hostnames
###################
if DEBUG2: print "Read Existing Hostnames"

hostnames = set(list_data.load_data(hosts_dir + hostname_filename))
IF_DATA_READ = 1

if DEBUG3: print "  #hostnames: %d" % (len(hostnames))


###################
## read entrance websites
###################
if DEBUG2: print "Read Entrance Web Sites"

if os.path.exists(PARAM_FILE):
  entrances = list_data.load_data(hosts_dir + entrance_filename)
else:
  entrances = list_data.load_data(hosts_dir + entrance_filename)
if DEBUG3: print "  #entrances: %d" % (len(entrances))

num = 0
for entrance in entrances:
  cnt = add_to_pool(entrance)
  num += cnt



###################
## Go Over Web Sites to Find Hostnames
###################
if DEBUG2: print "Go Over Web Sites to Find Hostnames"

# order = np.random.permutation(len(entrances))
# for idx in order:
#   entrance = entrances[idx]
#   ent_url = urlparse(entrance)
#   print "  %d: %s" % (idx, entrance)
cnt_url = 0
while num > 0:
  idx = random.randint(0, len(url_pool)-1)
  this_keys = url_pool.keys()
  this_key = this_keys[idx]
  if len(url_pool[this_key]) == 0: continue

  this_url = url_pool[this_key].pop()
  num -= 1

  if DEBUG3: print "  %d: %s" % (cnt_url, this_url)


  ## Get HTML content 
  ## (there some well-known problem of getting url in python, so I use curl instead)
  tmp_filename = "tmp.%f.txt" % (random.random())
  try:
    os.system("curl -k -s -m 30 %s > %s" % (this_url, tmp_filename) )
  except Exception as e:
    print "  [1] type: %s, msg: %s" % (type(e), e)
    # input('wait....')
    continue

  if os.path.exists(tmp_filename):
    fh = open(tmp_filename, 'r')
    html = fh.read()
    fh.close()
    os.system("rm -f %s" % (tmp_filename))
  else:
    continue

  ## Parse the web and get urls
  soup = BeautifulSoup(html, 'html.parser')
  # print(soup.prettify())
  for link in soup.find_all('a'):
    name = link.get('href')
    if name is None: continue
    m = re.search("http", name)
    if m is None: continue

    cnt = add_to_pool(name)
    num += cnt

    add_to_hostnames(name)
  
  cnt_url += 1
  if cnt_url % 10 == 0:
    store_output_files()

  print "    # host in pools: %d" %(len(url_pool))
  print "    # url in pools: %d" % (num)
  print "    # hostnames: %d" % (len(hostnames))
  time.sleep(0.2)

store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))
