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
projname = 'cdn_selection'
taskname = 'dns_cname_search'
progname = 'dns_cname_search'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

hostname_dir = '../../data/hostname_search/hostname/'
output_dir   = '../../data/' + taskname + '/dns_cname/'
hostname_filename        = 'hostnames.txt'
cname_dict_filename      = 'cnames.data'
cname_filename           = 'cnames.txt'
auth_dict_filename       = 'auth_dns.data'
auth_filename            = 'auth_dns.txt'
fail_cname_dict_filename = 'cnames.fail.data'
fail_cname_filename      = 'cnames.fail.txt'
fail_auth_dict_filename  = 'auth_dns.fail.data'
fail_auth_filename       = 'auth_dns.fail.txt'

cnames = {}
auths  = {}
cname_list = set()
auth_list  = set()
fail_cnames = {}
fail_auths  = {}
fail_cname_list = set()
fail_auth_list  = set()

wait_time = 15


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
  rcode = response.rcode()
  if rcode != dns.rcode.NOERROR:
    if DEBUG3: print "    error %s: %s" % (rcode, domain)

    if domain in fail_cnames:
      fail_cnames[domain].add(rcode)
    else:
      fail_cnames[domain] = set([rcode])

    fail_cname_list.add("%s|%d" % (domain, rcode))
    return ret

  ## check answers
  rrsets = response.answer
  for rrset in rrsets:
    for rr in rrset:
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
      if DEBUG3: print "    error %s: %s" % (rcode, sub)
      if sub in fail_auths:
        fail_auths[sub].add(rcode)
      else:
        fail_auths[sub] = set([rcode])

      fail_auth_list.add("%s|%d" % (sub, rcode))
      continue
      # if rcode == dns.rcode.NXDOMAIN:
      #   raise Exception('%s does not exist.' % sub)
      # else:
      #   raise Exception('Error %s' % dns.rcode.to_text(rcode))

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


def store_output_files():
  data.store_data(output_dir + cname_dict_filename, cnames)
  data.store_data(output_dir + auth_dict_filename, auths)
  list_data.store_data(output_dir + cname_filename, list(cname_list))
  list_data.store_data(output_dir + auth_filename, list(auth_list))
  data.store_data(output_dir + fail_cname_dict_filename, fail_cnames)
  data.store_data(output_dir + fail_auth_dict_filename, fail_auths)
  list_data.store_data(output_dir + fail_cname_filename, list(fail_cname_list))
  list_data.store_data(output_dir + fail_auth_filename, list(fail_auth_list))


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  store_output_files()
  os.system("touch %s" % (KILLED_IND_FILE))
  os.system("rm %s" % (RUNNING_IND_FILE))
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


## handle unresponsive nodes
def timeout_handler(signum, frame):
  raise Exception("end of time")
signal.signal(signal.SIGALRM, timeout_handler)



###################
## Main
###################
force_utf8_hack()
os.system("rm %s" % (DONE_IND_FILE))
os.system("rm %s" % (KILLED_IND_FILE))
os.system("touch %s" % (RUNNING_IND_FILE))


###################
## Load hostnames and done search
###################
if DEBUG2: print "Load Hostnames and Done Search"

if os.path.exists(PARAM_FILE):
  filename = PARAM_FILE
elif os.path.exists(hostname_dir + hostname_filename):
  filename = hostname_dir + hostname_filename
else:
  print "no input parameter: " + PARAM_FILE
  exit()

hostnames = list_data.load_data(filename)

cnames = data.load_data(output_dir + cname_dict_filename)
auths  = data.load_data(output_dir + auth_dict_filename)
cname_list = set(list_data.load_data(output_dir + cname_filename))
auth_list  = set(list_data.load_data(output_dir + auth_filename))
fail_cnames = data.load_data(output_dir + fail_cname_dict_filename)
fail_auths  = data.load_data(output_dir + fail_auth_dict_filename)
fail_cname_list = set(list_data.load_data(output_dir + fail_cname_filename))
fail_auth_list  = set(list_data.load_data(output_dir + fail_auth_filename))


###################
## Start DNS Lookup
###################
if DEBUG2: print "Start DNS Lookup"

cnt = 0
for hostname in hostnames:
  cnt = cnt + 1
  if DEBUG3: print("%d/%d: %s" % (cnt, len(hostnames), hostname))

  if hostname in cnames:
    if hostname in auths:
      if DEBUG3: print "  -- has been found"
      continue

  ###################
  ## Get CNAME -- method 2
  ###################
  if DEBUG2: print "  Get CNAME"

  signal.alarm(wait_time)
  try:
    this_cnames = get_cname2(hostname)
    if DEBUG3: print "    num=%d" % (len(this_cnames))

    ## add to cnames dict
    if hostname in cnames:
      cnames[hostname].update(this_cnames)
    else:
      cnames[hostname] = this_cnames

    ## add to list
    cname_list.update(this_cnames)

  except dns.resolver.NoAnswer as e:
    if DEBUG3: print "  no answer"
  except Exception as e:
    if DEBUG2: print "  " + str(type(e))


  ###################
  ## Get Authoritative DNS
  ###################
  if DEBUG2: print "  Get Authoritative DNS"

  signal.alarm(wait_time)
  try:
    this_auths = get_authoritative_nameserver(hostname)
    if DEBUG3: print "    num=%d" % (len(this_auths))

    ## add to authority dict
    for domain in this_auths:
      if domain in auths:
        auths[domain].update(this_auths[domain])
      else:
        auths[domain] = this_auths[domain]

      ## add to authority list
      auth_list.update(this_auths[domain])

    ## in case hostname's auth is not found
    if hostname not in auths:
      if DEBUG3: print "    hostname itself is not found"
      auths[hostname] = set()

  except Exception as e:
    if DEBUG2: print "  " + str(type(e))


  if cnt % 100 == 0:
    store_output_files()

  if DEBUG3:
    print "  #cnames=%d" % (len(cname_list))
    print "  #auth=%d" % (len(auth_list))

  signal.alarm(0)
  time.sleep(3)


###################
## store data
###################
if DEBUG2: print "Store Data"

store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))
