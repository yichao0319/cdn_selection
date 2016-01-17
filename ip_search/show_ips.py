#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, math, re, fnmatch, signal, time, locale
import list_data
import data

ips_filename = '../../data/ip_search/ips/ips.data'

ips = data.load_data(ips_filename)

cname_list = set()
dns_list = set()
ip_list = set()
for cname in ips:
    cname_list.add(cname)
    for dns in ips[cname]:
        dns_list.add(dns)
        for ip in ips[cname][dns]:
            ip_list.add(ip)

print "------------------------\n"
print "CNAMEs:"
print "\n".join(cname_list)
print "------------------------\n"
print "DNS:"
print "\n".join(dns_list)
print "------------------------\n"
print "IPs:"
print "\n".join(ip_list)

print "#CNAME=%d" % (len(cname_list))
print "#DNS=%d" % (len(dns_list))
print "#IP=%d" % (len(ip_list))


