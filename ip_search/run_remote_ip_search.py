#!/usr/bin/python
import sys, os, math, re, fnmatch, signal, time
import list_data
import data


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1


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



###################
## DEBUG
# exit()
###################


###################
## Main
###################

###################
## get PlanetLab nodes states
###################
if DEBUG2: print "Get PlanetLab Nodes"

if os.path.exists(plnode_dir + ready_node_filename): 
  filename = plnode_dir + ready_node_filename
else:
  filename = plnode_dir + deploy_node_filename

nodes = list_data.load_data(filename)


###################
## read cname and dns
###################
if DEBUG2: print "Read Data"

###################################################
## only CDN:
# os.system("cat " + cname_dir + cname_filename + " | grep \"cdn\|chinacache\|ccgslb\" > tmp.cdn_list.txt")
os.system("cat " + cname_dir + cname_filename + " | grep \"aka\|chinacache\|ccgslb\" > tmp.cdn_list.txt")
cnames = list_data.load_data("tmp.cdn_list.txt")
os.system("rm tmp.cdn_list.txt")

## only valid DNS:
# auths  = list_data.load_data(cname_dir + auth_filename)
auths  = list_data.load_data(dns_dir + dns_filename)
###################################################

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
## find remain CNAMEs
###################
# if DEBUG2: print "Find Remain CNAMEs"

# done_cnames = ips.keys()
# remain_cnames = list(set(cnames) - set(done_cnames))
# cnames = remain_cnames


###################
## Arrange Jobs
###################
if DEBUG2: print "Arrange Jobs"

njobs = math.ceil(len(cnames) * 1.0 / len(nodes))
if DEBUG3: print "  %d jobs per node" % (njobs)
# exit()


###################
## Generate Parameter Files
###################
if DEBUG2: print "Generate Parameter Files"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]

## assigned DNS 
list_data.store_data(PARAM_FILE_AUTH, auths)

## assigned CNAME
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  std = int(ni * njobs)
  end = int( min((ni+1)*njobs-1, len(cnames)-1) )
  if DEBUG3: print("------------------------\n  %d/%d: %s [%d-%d]" % (ni+1, len(nodes), node, std, end))

  while std >= len(cnames):
    # if DEBUG3: print("  no more cnames")
    # break;
    std = int(std - len(cnames))
    end = int( min(std+njobs-1, len(cnames)-1) )
  
  
  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  fp = open(PARAM_FILE_CNAME + "." + node, 'w')
  for idx in xrange(std, end+1):
    fp.write("%s\n" % (cnames[idx]));
  fp.close()



###################
## run jobs
###################
if DEBUG2: print "Run Jobs"

## Copy parameter-CNAME to the server
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s.{} %s@{}:~/%s/git_repository/%s/%s" % (filename, output_dir, wait_time, PARAM_FILE_CNAME, username, projname, taskname, PARAM_FILE_CNAME))

## Copy parameter-DNS to the server
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s %s@{}:~/%s/git_repository/%s/%s" % (filename, output_dir, wait_time, PARAM_FILE_AUTH, username, projname, taskname, PARAM_FILE_AUTH))

## Execute
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"cd %s/git_repository/%s; rm %s; nohup python ip_search.py &> %s &\" " % (filename, output_dir, wait_time, username, projname, taskname, NOHUP_LOG_FILE, NOHUP_LOG_FILE))

## rm parameters
os.system("rm %s" % (PARAM_FILE_AUTH))
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  os.system("rm %s.%s" % (PARAM_FILE_CNAME, node))


