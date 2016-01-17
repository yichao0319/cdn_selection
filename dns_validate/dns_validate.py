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
taskname = 'dns_validate'
progname = 'dns_validate'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
dns_dir       = '../../data/' + taskname + '/dns/'
cname_dir     = '../../data/dns_cname_search/dns_cname/'
output_dir    = '../../data/' + taskname + '/tmp_run/'

deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

auth_filename    = 'auth_dns.txt'
dns_filename     = 'valid_auth_dns.txt'
bad_dns_filename = 'invalid_auth_dns.txt'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 60

valid_dns = set()
invalid_dns = set()



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
  list_data.store_data(dns_dir + dns_filename, list(valid_dns))
  list_data.store_data(dns_dir + bad_dns_filename, list(invalid_dns))



def send_query_to_ns(hostname, nameserver):
  ret = set()

  query = dns.message.make_query(hostname, dns.rdatatype.A)
  response = dns.query.udp(query, nameserver, timeout=wait_time)

  rcode = response.rcode()
  if rcode != dns.rcode.NOERROR:
    print ("    error code %s" % (dns.rcode.to_text(rcode)))
    raise Exception("DNS Error Code")

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


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  if IF_DATA_READ:
    store_output_files()
  os.system("touch %s" % (KILLED_IND_FILE))
  os.system("rm %s" % (RUNNING_IND_FILE))
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)



###################
## Variables
###################

###################
## Main
###################
force_utf8_hack()
os.system("rm %s" % (DONE_IND_FILE))
os.system("rm %s" % (KILLED_IND_FILE))
os.system("touch %s" % (RUNNING_IND_FILE))


###################
## read dns candidate
###################
if DEBUG2: print "Read DNS Candidate"

cname = "www.baidu.com"

if os.path.exists(PARAM_FILE):
  auths  = list_data.load_data(PARAM_FILE)
else:
  auths  = list_data.load_data(cname_dir + auth_filename)

if DEBUG3: print "  #auth dns=%d" % (len(auths))


###################
## read done DNS
###################
if DEBUG2: print "Read Done DNS"

valid_dns = set(list_data.load_data(dns_dir + dns_filename))
invalid_dns = set(list_data.load_data(dns_dir + bad_dns_filename))
IF_DATA_READ = 1

if DEBUG3: print "  # done valid dns=%d" % (len(valid_dns))
if DEBUG3: print "  # done invalid dns=%d" % (len(invalid_dns))


###################
## DNS Query
###################
if DEBUG2: print "DNS Query"

###################
## DEBUG
# auths = ["123.240.255.47", "app.cnmo.com", "apple2.shec.edu.cn", "apple3.shec.edu.cn", "alpha2.bocach.gov.tw", "alleoyako.blog.fc2.com"]
# exit()
###################

cnt = 0
for auth in auths:
  ## the dns has been validated
  if auth in valid_dns:
    continue
  if auth in invalid_dns:
    continue

  cnt = cnt + 1

  ## get IP of auth DNS
  try:
    answers = dns.resolver.query(auth, 'A')
  # except dns.exception.Timeout as e:
  #   if DEBUG2: print "    [2] query DNS Timeout"
  #   continue
  except Exception as e:
    invalid_dns.add(auth)

    if DEBUG2: print "    [2] exception type %s: %s" % (type(e), e)
    continue


  for rdata in answers:
    if DEBUG3: print "  %d> cname: %s -> dns: %s [%s] (#valid=%d)" % (cnt, cname, auth, rdata.address, len(valid_dns))

    # time_cnt = 0
    # signal.alarm(wait_time)
    try:
      this_ips = send_query_to_ns(cname, rdata.address)
      valid_dns.add(auth)

    # except dns.exception.Timeout as e:
    #   if DEBUG2: print "    [1] query CNAME Timeout"

    except Exception as e:
      invalid_dns.add(auth)

      if DEBUG2: print "    [1]" + str(type(e)) + ": " + str(e)


  if cnt % 100 == 0:
    store_output_files()


###################
## store data
###################
if DEBUG2: print "Store Data"

# print "  "+"\n  ".join(list(hostnames))+"\n"
store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))
