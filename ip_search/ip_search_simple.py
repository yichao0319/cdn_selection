#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch
import time, locale, datetime
import socket
import dns, dns.name, dns.query, dns.resolver, dns.exception
import list_data


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
taskname = 'ip_search'
progname = 'ip_search_simple'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
ips_dir       = '../../data/' + taskname + '/ips/'
hostname_dir  = '../../data/hostname_search/hostname/'
cname_dir     = '../../data/dns_cname_search/dns_cname/'
# output_dir    = '../../data/' + taskname + '/tmp_run/'

hostname_filename    = 'hostnames.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

cname_filename = 'cnames.txt'
auth_filename  = 'auth_dns.txt'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 10


def send_query_to_ns(hostname, nameserver):
  ret = set()

  query = dns.message.make_query(hostname, dns.rdatatype.A)
  response = dns.query.udp(query, nameserver, timeout=2)

  rcode = response.rcode()
  if rcode != dns.rcode.NOERROR:
    print ("    error code %s" % (dns.rcode.to_text(rcode)))
    # pass
    return ret

  if len(response.authority) > 0:
    rrsets = response.authority
    print "authority:" + response.authority
  elif len(response.additional) > 0:
    rrsets = [response.additional]
    print "additional"
  else:
    rrsets = response.answer
    if DEBUG4: print "  #answers = %d" % (len(rrsets))

    for rrset in rrsets:
      if DEBUG4: print "    #sets in answers = %d" % (len(rrset))

      for rr in rrset:
        if rr.rdtype == dns.rdatatype.A:
          if DEBUG4: print "  Name = %s" % (rr.address)
          
          ret.add(rr.address)

        elif rr.rdtype == dns.rdatatype.CNAME:
          if DEBUG4: print "  Name = %s" % (rr.target)

        else:
          if DEBUG4: print "  rdtype = %s" % (dns.rdatatype.to_text(rr.rdtype))

  return ret

## handle unresponsive nodes
def timeout_handler(signum, frame):
  time_cnt = time_cnt + 1
  if time_cnt > wait_time:
    print "xxxxx"
    raise Exception("end of time")
  else:
    print "hhhhh"
    sys.stdout.write("\r\x1b[k"+str(time_cnt))
signal.signal(signal.SIGALRM, timeout_handler)


###################
## Variables
###################

###################
## Main
###################

###################
## read cname and dns
###################
if DEBUG2: print "Read Data"

cnames = list_data.load_data(cname_dir + cname_filename)
auths  = list_data.load_data(cname_dir + auth_filename)

if DEBUG3: print "  #cnames=%d" % (len(cnames))
if DEBUG3: print "  #auth dns=%d" % (len(auths))


###################
## DNS Query
###################
if DEBUG2: print "DNS Query"

ips = set()
for cname in cnames:
  for auth in auths:

    # cname = "ghs.google.com"
    # auth  = "ns1.google.com"

    answers = dns.resolver.query(auth, 'A')
    for rdata in answers:
      if DEBUG3: print "  cname %s -> dns: %s [%s]" % (cname, auth, rdata.address)

      # time_cnt = 0
      # signal.alarm(wait_time)
      try:
        this_ips = send_query_to_ns(cname, rdata.address)

        if len(this_ips) > 0:
          print "  " + "\n  ".join(list(this_ips))
          ips.update(this_ips)
      except dns.exception.Timeout as e:
        if DEBUG2: print "  [1] DNS Timeout"
        
      except Exception as e:
        if DEBUG2: print "  [1]" + str(type(e)) + ": " + str(e)

      # signal.alarm(0)
    # exit()



