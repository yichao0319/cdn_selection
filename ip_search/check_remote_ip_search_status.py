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
output_dir    = '../../data/' + taskname + '/tmp_run/'

hostname_filename    = 'hostnames.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

cname_filename   = 'cnames.txt'
auth_filename    = 'auth_dns.txt'
ip_dict_filename = 'ips.data'
ip_filename      = 'ips.txt'

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


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  if IF_DATA_READ:
    store_output_files()
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


## handle unresponsive nodes
def timeout_handler(signum, frame):
  raise Exception("end of time")
signal.signal(signal.SIGALRM, timeout_handler)


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

nodes = list_data.load_data(plnode_dir + deploy_node_filename)


###################
## read cname and dns
###################
if DEBUG2: print "Read Data"

cnames = list_data.load_data(cname_dir + cname_filename)
auths  = list_data.load_data(cname_dir + auth_filename)

if DEBUG3: print "  #cnames=%d" % (len(cnames))
if DEBUG3: print "  #auth dns=%d" % (len(auths))


###################
## get data
###################
if DEBUG2: print "Get Existing IP Data"

ips = data.load_data(ips_dir + ip_dict_filename)
IF_DATA_READ = 1

if DEBUG3: print "  #cnames=%d" % (len(ips))


###################
## Check Remote Status -- Prepare Tmp Directory
###################
if DEBUG2: print "Prepare Tmp Directory"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]
nodes_running = []
nodes_ready   = []
nodes_bad     = []
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  # if DEBUG3: print "------------------------\n  %d/%d: %s" % (ni+1, len(nodes), node)

  ## create tmp directory
  tmp_dir = ("tmp.%s" %(node))
  if os.path.exists(tmp_dir): os.system("rm -rf %s" % tmp_dir)
  os.system("mkdir %s" % tmp_dir)


###################
## Copy remote output files
###################
if DEBUG2: print "Copy Remote Output Files"  

## host names
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/data/%s/ips ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname))
## job done indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, DONE_IND_FILE))
## job running indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, RUNNING_IND_FILE))
## job killed indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, KILLED_IND_FILE))
## remove output_dir
os.system("rm -rf " + output_dir)


###################
## Check Status
###################
if DEBUG2: print "Check Status"

for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  tmp_dir = ("tmp.%s" %(node))

  ## if node is ready for the next job
  if os.path.exists("%s/%s" % (tmp_dir, DONE_IND_FILE)):
    nodes_ready.append(node)
  elif os.path.exists("%s/%s" % (tmp_dir, KILLED_IND_FILE)):
    nodes_ready.append(node)
  elif os.path.exists("%s/%s" % (tmp_dir, RUNNING_IND_FILE)):
    nodes_running.append(node)
  else:
    nodes_ready.append(node)

  ## read files
  this_ips = data.load_data(tmp_dir + "/ips/" + ip_dict_filename)
  ips = merge_ips(ips, this_ips)
  if DEBUG3: print "    #ips: %d, total: %d" % (len(this_ips), len(ips))
  
  ## remove tmp_dir
  os.system("rm -rf %s" % (tmp_dir))
  time.sleep(0.1)


if DEBUG3:
  print "  Ready Nodes: "
  print "    "+"\n    ".join(nodes_ready)
  print "  Running Nodes: "
  print "    "+"\n    ".join(nodes_running)
  print "  Bad Nodes: "
  print "    "+"\n    ".join(nodes_bad)
  print "  #cnames: %d" % (len(ips))


## Update local files
store_output_files()
if len(nodes_ready) > 0:
  list_data.store_data(plnode_dir + ready_node_filename, nodes_ready)


