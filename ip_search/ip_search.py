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
taskname = 'ip_search'
progname = 'ip_search'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE_CNAME = 'tmp.' + progname + '.param1.txt'
PARAM_FILE_AUTH  = 'tmp.' + progname + '.param2.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
ips_dir       = '../../data/' + taskname + '/ips/'
cname_dir     = '../../data/dns_cname_search/dns_cname/'
dns_dir       = '../../data/dns_validate/dns/'
output_dir    = '../../data/' + taskname + '/tmp_run/'

deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

cname_filename   = 'cnames.txt'
auth_filename    = 'auth_dns.txt'
ip_dict_filename = 'ips.data'
ip_filename      = 'ips.txt'
dns_filename     = 'valid_auth_dns.txt'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 10

ips = {}


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



def merge_ips(ips1, ips2):
  for cname in ips2:
    if cname in ips1:
      for dns in ips2[cname]:
        if dns in ips1[cname]:
          ips1[cname][dns].update(ips2[cname][dns])
        else:
          ips1[cname][dns] = ips2[cname][dns]
    else:
      ips1[cname] = ips2[cname]

  return ips1


def dict_ips_to_list(ips):
  ret = []
  for cname in ips:
    for dns in ips[cname]:
      for ip in ips[cname][dns]:
        ret.append("%s|%s|%s" % (cname, dns, ip))

  return ret


def store_output_files():
  # print len(ips)
  data.store_data(ips_dir + ip_dict_filename, ips)
  ip_list = dict_ips_to_list(ips)
  list_data.store_data(ips_dir + ip_filename, ip_list)





def send_query_to_ns(hostname, nameserver):
  ret = set()

  query = dns.message.make_query(hostname, dns.rdatatype.A)
  response = dns.query.udp(query, nameserver, timeout=wait_time)

  rcode = response.rcode()
  if rcode != dns.rcode.NOERROR:
    print ("    error code %s" % (dns.rcode.to_text(rcode)))
    # pass
    return ret

  if len(response.authority) > 0:
    rrsets = response.authority
    print "authority"
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
signal.signal(signal.SIGTERM, signal_handler)



# ## handle unresponsive nodes
# def timeout_handler(signum, frame):
#   time_cnt = time_cnt + 1
#   if time_cnt > wait_time:
#     print "xxxxx"
#     raise Exception("end of time")
#   else:
#     print "hhhhh"
#     sys.stdout.write("\r\x1b[k"+str(time_cnt))
# signal.signal(signal.SIGALRM, timeout_handler)


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
## read cname and dns
###################
if DEBUG2: print "Read Data"

if os.path.exists(PARAM_FILE_CNAME):
  cnames = list_data.load_data(PARAM_FILE_CNAME)
else:
  cnames = list_data.load_data(cname_dir + cname_filename)

if os.path.exists(PARAM_FILE_AUTH):
  auths  = list_data.load_data(PARAM_FILE_AUTH)
elif os.path.exists(dns_dir + dns_filename):
  auths  = list_data.load_data(dns_dir + dns_filename)
else:
  auths  = list_data.load_data(cname_dir + auth_filename)

if DEBUG3: print "  #cnames=%d" % (len(cnames))
if DEBUG3: print "  #auth dns=%d" % (len(auths))


###################
## read done IPs
###################
if DEBUG2: print "Read Done IPs"

ips = data.load_data(ips_dir + ip_dict_filename)
IF_DATA_READ = 1

if DEBUG3: print "  #cnames=%d" % (len(ips))


###################
## DNS Query
###################
if DEBUG2: print "DNS Query"

###################
## DEBUG
# cname = "0.site.51zx.com"
# cnames = ["0.site.51zx.com"]
# auths = ["DNS.RAIDC.COM", "agri-dns02.agri.org.cn", "agri-dns02.agri.org.cn"]
# exit()
###################

cnt = 0
for cname in cnames:
  for auth in auths:
    ## the cname has been searched in auth DNS
    if cname in ips:
      if auth in ips[cname]:
        continue

    cnt = cnt + 1

    ## get IP of auth DNS
    try:
      answers = dns.resolver.query(auth, 'A')
    except Exception as e:
      if DEBUG2: print "    [2] exception type %s: %s" % (type(e), e)
      continue


    for rdata in answers:
      if DEBUG3: print "  %d> cname: %s -> dns: %s [%s] (#ip=%d)" % (cnt, cname, auth, rdata.address, len(ips))

      # time_cnt = 0
      # signal.alarm(wait_time)
      this_ips = set()
      try:
        this_ips = send_query_to_ns(cname, rdata.address)
      except Exception as e:
        if DEBUG2: print "    [1]" + str(type(e)) + ": " + str(e)


      if len(this_ips) > 0:
        print "    " + ("\n  ".join(list(this_ips)))
        tmp = {cname: {auth: this_ips} }
        ips = merge_ips(ips, tmp)



    if cnt % 100 == 0:
      store_output_files()

  store_output_files()

# exit();

###################
## store data
###################
if DEBUG2: print "Store Data"

# print "  "+"\n  ".join(list(hostnames))+"\n"
store_output_files()
os.system("touch %s" % (DONE_IND_FILE))
os.system("rm %s" % (RUNNING_IND_FILE))
