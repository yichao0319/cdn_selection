#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch
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
progname = 'ip_geolocate'

ips_dir    = '../../data/ip_search/ips/'
geo_db_dir = '../../data/' + taskname + '/database/'
geo_dir    = '../../data/' + taskname + '/geo/'

ip_dict_filename   = 'ips.data'
geo_db_filename    = 'dbip-city-2016-01.csv'
my_geo_db_filename = 'geo_db.data'
geo_dict_filename  = 'geo.data'
geo_filename       = 'geo.csv'
exist_ip_filename  = 'exist_ip_geo.data'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 60

cdns = ["wscdns", "cdn20", "chinacache", "ccgslb", "fastcdn", "cloudcdn", "yunjiasu-cdn", "cctvcdn", "jiashule", "verycdn", "cloudflare"]
## geo: cdn_provider -> ip -> location
geo = {}
located_ips = {}


def merge_geo_data(geo1, geo2):
  for provider in geo2:
    if provider in geo1:
      for ip in geo2[provider]:
        geo1[provider][ip] = geo2[provider][ip]

    else:
      geo1[provider] = geo2[provider]

  return geo1


def store_output_files():
  data.store_data(geo_dir + geo_dict_filename, geo)
  geo_list = geo_dict_to_csv(geo)
  list_data.store_data(geo_dir + geo_filename, geo_list)
  data.store_data(geo_dir + exist_ip_filename, located_ips)


def geo_dict_to_csv(geo):
  geo_list = ["CDN Provider,IP,Address"]
  for provider in geo:
    for ip in geo[provider]:
      geo_list.append("%s,%s,%s" % (provider, ip, geo[provider][ip]))
  return geo_list


def ip2int(ip):
  v = re.split("\.", ip)
  ret = ((int(v[0])*256 + int(v[1]))*256 + int(v[2]))*256 + int(v[3])
  return ret


def read_ip2geo_db(filename):
  geo_db = [];
  fh = open(filename, 'r')
  for line in fh:
    line = line.rstrip()
    if DEBUG4: print line
    # m = re.search("\"(\d+\.\d+\.\d+\.\d+)\",\"(\d+\.\d+\.\d+\.\d+)\",\"(\.+?)\",\"(\.+?)\",\"(\.+?)\"", line)
    m = re.search("\"(\d+\.\d+\.\d+\.\d+)\",\"(\d+\.\d+\.\d+\.\d+)\",\"(.*?)\",\"(.*?)\",\"(.*?)\"", line)

    if m is not None:
      record = list(m.groups())
      record.append(ip2int(record[0]))
      record.append(ip2int(record[1]))
      geo_db.append(record)
      if DEBUG4: print record
    else:
      ## skip IPv6
      break
      # raise Exception("Fail to read IP2GEO DB: " + line)

  fh.close()
  return geo_db


def search_in_db(ip, geo_db):
  ipval = ip2int(ip)
  for record in geo_db:
    if ipval >= record[5] and ipval <= record[6]:
      address = record[4] + " " + record[3] + " " +  record[2]
      return address
  raise Exception("Cannot find IP: " + ip)


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  store_output_files()
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

###################
## read ips
###################
if DEBUG2: print "Read IPs"

ips = data.load_data(ips_dir + ip_dict_filename)

if DEBUG3: print "  #cnames: %d" % (len(ips))


###################
## Read IP to Geolocation Database
###################
if DEBUG2: print "Read IP to Geolocation Database"

if os.path.exists(geo_db_dir + my_geo_db_filename):
  if DEBUG3: print "  read from my data structure"

  geo_db = data.load_data(geo_db_dir + my_geo_db_filename)

else:
  if DEBUG3: print "  read from orig db"

  geo_db = read_ip2geo_db(geo_db_dir + geo_db_filename)
  data.store_data(geo_db_dir + my_geo_db_filename, geo_db)

located_ips = data.load_data(geo_dir + exist_ip_filename)
if DEBUG3: print "  # records=%d" % (len(geo_db))


# try:
#   address = search_in_db("8.8.8.8", geo_db)
#   print address
# except Exception as e:
#   print "  [1] Search Exception " + type(e) + ": " + e

###################
## Locate IP
###################
if DEBUG2: print "Locate IP"

for target in cdns:
  if DEBUG3: print "  CDN: " + target

  for cdn in ips:
    m = re.search(target, cdn)
    if m is None: continue

    for dns in ips[cdn]:
      for ip in ips[cdn][dns]:
        
        if ip in located_ips:
          address = located_ips[ip]
        else:
          try:
            address = search_in_db(ip, geo_db)
            if DEBUG3: print "    " + ip + ": " + address
          except Exception as e:
            print "    [1] Search Exception " + type(e) + ": " + e
            continue
          
          located_ips[ip] = address
          
        this_geo = {target: {ip: address}}
        merge_geo_data(geo, this_geo)

store_output_files()


