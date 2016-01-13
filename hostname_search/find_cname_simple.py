#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, signal, random, re, fnmatch
import time, locale, datetime
import socket
import dns, dns.name, dns.query, dns.resolver, dns.exception
from list_data import *


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1
DEBUG4 = 1


def get_ip(domain):
  ret = set()
  answers = dns.resolver.query(hostname, "A")
  if DEBUG4: print '  query name:', answers.qname, ' for A, num ans. =', len(answers)
  
  for rdata in answers:
    if DEBUG4: print '    target address:', rdata.address
    ret.add(rdata.address)


def get_cname1(domain):
  ret = set()

  answers = dns.resolver.query(hostname, "CNAME")
  if DEBUG4: print '  query name:', answers.qname, ' for CNAME, num ans. =', len(answers)

  for rdata in answers:
    if DEBUG4: print '    target address:', rdata.target
    ret.add(rdata.target.to_text())
  return ret
  

def get_cname2(domain):
  ret = set()

  ## find default DNS server
  default = dns.resolver.get_default_resolver()
  nameserver = default.nameservers[0]

  ## send query
  query = dns.message.make_query(domain, dns.rdatatype.CNAME)
  response = dns.query.udp(query, nameserver) ## response is of type: dns.message.Message

  ## check if getting error 
  print "--- check if getting error "
  rcode = response.rcode()
  if rcode != dns.rcode.NOERROR:
    if rcode == dns.rcode.NXDOMAIN:
      raise Exception('%s does not exist.' % domain)
    else:
      raise Exception('Error %s' % dns.rcode.to_text(rcode))

  ## check answers
  print "--- check answers"
  rrsets = response.answer
  for rrset in rrsets:
    print rrset
    for rr in rrset:
      print rr
      if rr.rdtype == dns.rdatatype.CNAME:
        if DEBUG4: print "  Name = %s" % (rr.target)
        ret.add(rr.target.to_text())
      else:
        pass

  return ret



def get_authoritative_nameserver(domain):
  ret = dict()
  n = dns.name.from_text(domain)
  
  ## find default DNS server
  default = dns.resolver.get_default_resolver()
  nameserver = default.nameservers[0]


  ## for each sub domain, find the authoritative DNS
  n = domain.split('.')
  for i in xrange(len(n), 0, -1):
    sub = '.'.join(n[i-1:])
    if DEBUG4: print('  Looking up %s on %s' % (sub, nameserver))

    query = dns.message.make_query(sub, dns.rdatatype.NS)
    response = dns.query.udp(query, nameserver)  ## response is of type: dns.message.Message

    rcode = response.rcode()
    if rcode != dns.rcode.NOERROR:
      if rcode == dns.rcode.NXDOMAIN:
        raise Exception('%s does not exist.' % sub)
      else:
        raise Exception('Error %s' % dns.rcode.to_text(rcode))

    if len(response.authority) > 0:
      rrsets = response.authority
    elif len(response.additional) > 0:
      rrsets = [response.additional]
    else:
      rrsets = response.answer

    # Handle all RRsets, not just the first one
    for rrset in rrsets:
      for rr in rrset:  
        ## rr = an RRset is a named rdataset,
        ##      an rdataset is a set of rdatas of a given type and class
        if rr.rdtype == dns.rdatatype.SOA:
          if DEBUG4: print('    Same server is authoritative for %s' % sub)

          if sub in ret:
            ret[sub].add(sub)
          else:
            ret[sub] = set([sub])
        
        elif rr.rdtype == dns.rdatatype.A:
          ns = rr.items[0].address
          if DEBUG4: print('    Glue record for %s: %s' % (rr.name, ns))
        
        elif rr.rdtype == dns.rdatatype.NS:
          authority = rr.target
          # ns = default.query(authority).rrset[0].to_text()
          # print('%s [%s] is authoritative for %s; ttl %i' % (authority, ns, sub, rrset.ttl))
          # print('%s' % (authority))
          if DEBUG4: print('    Authority: %s' % (authority))
          # result = rrset

          if sub in ret:
            ret[sub].add(authority.to_text())
          else:
            ret[sub] = set([authority.to_text()])
        else:
          # IPv6 glue records etc
          if DEBUG4: print "    Skip code %d" % (rr.rdtype)
          pass

  return ret


###################
## Variables
###################
hostname = "branda0717.pixnet.net"
# hostname = "google.com"
# hostname = "mail.google.com"


###################
## Main
###################


###################
## Get IP
###################
# if DEBUG2: print "Get IP"

try:
  ips = get_ip(hostname)
except Exception as e:
  if DEBUG2: print "  " + str(type(e)) + ": " + str(e)


###################
## Get CNAME -- method 1
###################
if DEBUG2: print "Get CNAME -- method 1"

try:
  cnames = get_cname1(hostname)
except dns.resolver.NoAnswer as e:
  if DEBUG4: print "  no answer"
except Exception as e:
  if DEBUG4: print "  " + str(type(e)) + ": " + str(e)


###################
## Get CNAME -- method 2
###################
if DEBUG2: print "Get CNAME -- method 2"

try:
  cnames = get_cname2(hostname)
except dns.resolver.NoAnswer as e:
  if DEBUG3: print "  no answer"
except Exception as e:
  if DEBUG2: print "  " + str(type(e)) + ": " + str(e)


###################
## Get Authoritative DNS
###################
if DEBUG2: print "Get Authoritative DNS"

try:
  auths = get_authoritative_nameserver(hostname)
  if DEBUG3: print auths
except Exception as e:
  if DEBUG2: print "  " + str(type(e)) + ": " + str(e)

