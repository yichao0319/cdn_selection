#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch, math, operator, requests, json
import time, locale, datetime
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
hostname_file    = '../../data/hostname_search/hostname/hostnames.txt'
auth_file        = '../../data/dns_cname_search/dns_cname/auth_dns.txt'
cname_file       = '../../data/dns_cname_search/dns_cname/cnames.txt'
valid_dns_file   = '../../data/dns_validate/dns/valid_auth_dns.txt'
ip_data_file     = '../../data/ip_search/ips/ips.data'
ip_geo_data_file = '../../data/ip_geolocate/geo/geo_ipinfo.data'

output_dir = '../../data/cdn_collection_analyze/data/'
num_ips_cname_filename = 'num_ips_cname_dist.txt'
num_ips_provider_filename = 'num_ips_provider_dist'


providers = ["wscdns", "cdn20", "chinacache", "ccgslb", "fastcdn", "cloudcdn", "yunjiasu-cdn", "cctvcdn", "jiashule", "verycdn", "cloudflare"]


###################
## DEBUG
# list_data.store_data("./tmp.txt", map(str, [11, 22]))
# exit()
###################


###################
## Main
###################

###################
## Load Data
###################
if DEBUG2: print "Load Data"

hostnames   = list_data.load_data(hostname_file)
cnames      = list_data.load_data(cname_file)
auths       = list_data.load_data(auth_file)
valid_dns   = list_data.load_data(valid_dns_file)
ips         = data.load_data(ip_data_file)
geos        = data.load_data(ip_geo_data_file)

if DEBUG3: print "  #hosts=%d" % (len(hostnames))
if DEBUG3: print "  #cnames=%d" % (len(cnames))
if DEBUG3: print "  #auth dns=%d" % (len(auths))
if DEBUG3: print "  #valid dns=%d" % (len(valid_dns))
if DEBUG3: print "  #cdn with ip=%d" % (len(ips))
if DEBUG3: print "  #located ips=%d" % (len(geos))



###################
## #IPs per CNAME 
###################
if DEBUG2: print "#IPs per CNAME/Provider"

num_cnames_provider = {}  ## number of CNAMEs per provider
num_ips_cname       = []  ## number of IPs per CNAME
ips_provider        = {}  ## IPs of each CDN provider
num_ips_provider    = {}  ## number of IPs per CNAME of each CDN provider
for provider in providers:
  ips_provider[provider] = set()
  num_ips_provider[provider] = []
  num_cnames_provider[provider] = set()

for cdn in ips:
  ips_cdn = set()
  for dns in ips[cdn]:
    ips_cdn.update(ips[cdn][dns])

  num_ips_cname.append(len(ips_cdn))

  for provider in providers:
    m = re.search(provider, cdn)
    if m is not None:
      ips_provider[provider].update(ips_cdn)
      num_ips_provider[provider].append(len(ips_cdn))
      num_cnames_provider[provider].add(cdn)

list_data.store_data(output_dir + num_ips_cname_filename, map(str, num_ips_cname))


## number of cnames per provider
fh = open(output_dir + "num_cnames_per_provider.txt", 'w')
for provider in providers:
  fh.write("%d\n" % (len(num_cnames_provider[provider])))
fh.close()


## number of IPs per provider
cnt = []
for provider in providers:
  print "  %s: #cnames=%d" % (provider, len(num_ips_provider[provider]))
  print "  %s: #unique ips=%d" % (provider, len(ips_provider[provider]))
  list_data.store_data(output_dir + num_ips_provider_filename + "." + provider + ".txt", map(str, num_ips_provider[provider]))

  cnt.append(len(ips_provider[provider]))

fh = open(output_dir + num_ips_provider_filename + ".txt", 'w')
fh.write("\n".join(map(str,cnt)))
fh.close()


## find duplicate IPs across Provider
dup_ips = {}
fh = open(output_dir + "dup_ips_cnt.txt", 'w')
print "Find Duplicate IPs across Provider"
for pi1 in xrange(0, len(providers)):
  fh.write(providers[pi1])
  for pi2 in xrange(0, len(providers)):
    if pi1 == pi2: 
      fh.write(",0")
      continue

    provider1 = providers[pi1]
    provider2 = providers[pi2]

    common_ips = ips_provider[provider1] & ips_provider[provider2]
    fh.write(",%d" % (len(common_ips)))
    if len(common_ips) > 0:
      print "  #common ips in %s and %s: %d (%d, %d)" % (provider1, provider2, len(common_ips), len(ips_provider[provider1]), len(ips_provider[provider2]))
      
    for this_ip in common_ips:
      if this_ip not in dup_ips:
        dup_ips[this_ip] = set()
      dup_ips[this_ip].add(provider1)
      dup_ips[this_ip].add(provider2)
  fh.write("\n")
fh.close()


# fh = open(output_dir + "dup_ips.txt", "w")
# for this_ip in dup_ips:
#   fh.write("%s,%s,%d," % (this_ip, located_ips[this_ip], len(dup_ips[this_ip])))
#   fh.write(",".join(dup_ips[this_ip]))
#   fh.write("\n")
# fh.close()


###################
## Location of providers
###################
if DEBUG2: print "Locations of Providers"

locations = {'all': {}}
locations['all']['city'] = {}
locations['all']['country'] = {}
locations['all']['as'] = {}
locations['all']['isp'] = {}

for provider in geos:
  ## initialization
  locations[provider] = {}
  locations[provider]['city'] = {}
  locations[provider]['country'] = {}
  locations[provider]['as'] = {}
  locations[provider]['isp'] = {}

  ## find ips belong to provider
  for ip in geos[provider]:

    ## put to the data
    ## Country
    country = geos[provider][ip]['country']
    if country in locations['all']['country']:
      locations['all']['country'][country] += 1
    else:
      locations['all']['country'][country] = 1
    
    if country in locations[provider]['country']:
      locations[provider]['country'][country] += 1
    else:
      locations[provider]['country'][country] = 1

    ## AS
    aas = geos[provider][ip]['as']
    if aas in locations['all']['as']:
      locations['all']['as'][aas] += 1
    else:
      locations['all']['as'][aas] = 1

    if aas in locations[provider]['as']:
      locations[provider]['as'][aas] += 1
    else:
      locations[provider]['as'][aas] = 1
    
    ## ISP
    isp = geos[provider][ip]['isp']
    if isp in locations['all']['isp']:
      locations['all']['isp'][isp] += 1
    else:
      locations['all']['isp'][isp] = 1
    if isp in locations[provider]['isp']:
      locations[provider]['isp'][isp] += 1
    else:
      locations[provider]['isp'][isp] = 1

    if country == 'CN':
      city = geos[provider][ip]['city']
      
      if city in locations['all']['city']:
        locations['all']['city'][city] += 1
      else:
        locations['all']['city'][city] = 1
        
      if city in locations[provider]['city']:
        locations[provider]['city'][city] += 1
      else:
        locations[provider]['city'][city] = 1

properties = ['country', 'city', 'as', 'isp']
for target in properties:
  fh = open(output_dir + 'location-' + target + '.txt', 'w')
  sorted_locations = sorted(locations['all'][target].items(), key=operator.itemgetter(1), reverse=True)

  fh.write("#%s, all" % target)
  for provider in providers:
    fh.write(", %s" % (provider))
  fh.write("\n")

  for t in sorted_locations:
    t0 = t[0].encode('utf-8')
    fh.write("\"%s\", %d" % (t0, t[1]))
    # print t0, str(locations['all'][target][t0]), 
    for provider in providers:
      if t0 in locations[provider][target]: 
        fh.write(", %d" %(locations[provider][target][t0]))
        # print str(locations[provider][target][t0]),
      else:
        fh.write(", 0")
        # print "0",
    fh.write("\n")
    # print ""
  fh.close()
