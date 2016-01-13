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

hostname_filename    = 'hostnames.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

auth_filename    = 'auth_dns.txt'
dns_filename     = 'valid_auth_dns.txt'
bad_dns_filename = 'invalid_auth_dns.txt'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 10

valid_dns = set()
invalid_dns = set()


def store_output_files():
  list_data.store_data(dns_dir + dns_filename, list(valid_dns))
  list_data.store_data(dns_dir + bad_dns_filename, list(invalid_dns))


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
## read done DNS
###################
if DEBUG2: print "Read Done DNS"

valid_dns = set(list_data.load_data(dns_dir + dns_filename))
invalid_dns = set(list_data.load_data(dns_dir + bad_dns_filename))
IF_DATA_READ = 1

if DEBUG3: print "  # done valid dns=%d" % (len(valid_dns))
if DEBUG3: print "  # done invalid dns=%d" % (len(invalid_dns))


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
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/data/%s/dns ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname))
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
  this_valid_dns = set(list_data.load_data(tmp_dir + "/dns/" + dns_filename))
  valid_dns.update(this_valid_dns)
  this_invalid_dns = set(list_data.load_data(tmp_dir + "/dns/" + bad_dns_filename))
  invalid_dns.update(this_invalid_dns)

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
  print "  #valid dns: %d" % (len(valid_dns))
  print "  #invalid dns: %d" % (len(invalid_dns))


## Update local files
store_output_files()
if len(nodes_ready) > 0:
  list_data.store_data(plnode_dir + ready_node_filename, nodes_ready)


