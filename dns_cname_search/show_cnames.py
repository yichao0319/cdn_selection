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

output_dir   = '../../data/' + taskname + '/dns_cname/'
cname_dict_filename      = 'cnames.data'
cname_filename           = 'cnames.txt'
auth_dict_filename       = 'auth_dns.data'
auth_filename            = 'auth_dns.txt'
fail_cname_dict_filename = 'cnames.fail.data'
fail_cname_filename      = 'cnames.fail.txt'
fail_auth_dict_filename  = 'auth_dns.fail.data'
fail_auth_filename       = 'auth_dns.fail.txt'


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


###################
## Main
###################
force_utf8_hack()

cnames = data.load_data(output_dir + cname_dict_filename)
auths  = data.load_data(output_dir + auth_dict_filename)
cname_list = set(list_data.load_data(output_dir + cname_filename))
auth_list  = set(list_data.load_data(output_dir + auth_filename))
fail_cnames = data.load_data(output_dir + fail_cname_dict_filename)
fail_auths  = data.load_data(output_dir + fail_auth_dict_filename)
fail_cname_list = set(list_data.load_data(output_dir + fail_cname_filename))
fail_auth_list  = set(list_data.load_data(output_dir + fail_auth_filename))

# data_cname_list = set()
# for hostname in cnames:
#     # print "  " + hostname
#     data_cname_list.update(cnames[hostname])

# print "  # hostnames   = %d" % (len(cnames))
# print "  # cnames      = %d" % (len(data_cname_list))
# print "  # cnames list = %d" % (len(cname_list))

# print "\n".join(data_cname_list)


data_auth_list = set()
for domain in auths:
    print "  " + domain
    data_auth_list.update(auths[domain])

print "  # domains  = %d" % (len(auths))
print "  # dns      = %d" % (len(data_auth_list))
print "  # dns list = %d" % (len(auth_list))

print "\n".join(data_auth_list)
