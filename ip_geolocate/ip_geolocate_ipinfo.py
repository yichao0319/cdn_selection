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
geo_dict_filename  = 'geo_ipinfo.data'
geo_filename       = 'geo_ipinfo.csv'
ipinfo_db_filename = 'ipinfo_db.data'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 60

cdns = ["wscdns", "cdn20", "chinacache", "ccgslb", "fastcdn", "cloudcdn", "yunjiasu-cdn", "cctvcdn", "jiashule", "verycdn", "cloudflare"]
## geo: cdn_provider -> ip -> location
geo = {}
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
  # data.store_data(geo_db_dir + ipinfo_db_filename, located_ips)


def geo_dict_to_csv(geo):
  geo_list = ["CDN Provider,IP,Lat,Lng,Address,AS,ISP"]
  for provider in geo:
    for ip in geo[provider]:
      addr = parse_ipinfo_data(geo[provider][ip])

      geo_list.append(("%s,%s,%s,%s %s %s,%s,%s" % (provider, ip, addr['loc'], addr['city'], addr['region'], addr['country'], addr['as'], addr['isp'])).encode('utf-8'))
  return geo_list


def parse_ipinfo_data(record):
  ret = {}
  properties = ["ip", "hostname", "city", "region", "country", "loc", "postal"]
  for p in properties:
    if p in record:
      ret[p] = record[p]
    else:
      ret[p] = ''

    if p == "city" and ret[p] == "" and 'country' in record:
      ret[p] = record['country']

    # if ret[p] == '' and p == "country":
    #   print p
    #   print record
    #   ok = raw_input('')

  ret['as'] = ''
  ret['isp'] = ''
  if 'org' in record:
    org = record['org']
    m = re.search("(AS\d+)\s+(.+)", org)
    if m is not None:
      # print org
      ret['as']  = m.group(1)
      ret['isp'] = m.group(2)

  return ret


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
force_utf8_hack()

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

located_ips = data.load_data(geo_db_dir + ipinfo_db_filename)

if DEBUG3: print "  # records=%d" % (len(located_ips))


# try:
#   address = search_in_db("8.8.8.8", geo_db)
#   print address
# except Exception as e:
#   print "  [1] Search Exception " + type(e) + ": " + e

###################
## Locate IP
###################
if DEBUG2: print "Locate IP"

num_unknown_ip = 0
for target in cdns:
  if DEBUG3: print "  CDN: " + target

  for cdn in ips:
    m = re.search(target, cdn)
    if m is None: continue

    for dns in ips[cdn]:
      for ip in ips[cdn][dns]:

        found = -1
        if ip not in located_ips:
          found = 100
          while found > 0:
            try:
              addressr = requests.get('http://ipinfo.io/' + ip)
              located_ips[ip] = r.json()
              # if DEBUG3: print "    " + ip + ": " + address
              found = -1
            except Exception as e:
              print "    [1] Search Exception " + type(e) + ": " + e
              found -= 1

            if found <= 0:
              num_unknown_ip += 1

        if found == 0:
          continue

        address = parse_ipinfo_data(located_ips[ip])

        this_geo = {target: {ip: address}}
        merge_geo_data(geo, this_geo)

store_output_files()

print "[NOTED] number of unknown IPs: %d" % (num_unknown_ip)

