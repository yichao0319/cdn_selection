#!/usr/bin/python
import sys, os, math, re, fnmatch, signal, time
import list_data


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1


###################
## Variables
###################
username = 'cuhk_cse_02'
projname = 'cdn_selection'
taskname = 'hostname_search'
progname = 'parse_links'
dirname  = 'hostname'

plnode_dir    = '../../data/' + taskname + '/plnode/'
hosts_dir     = '../../data/' + taskname + '/' + dirname + '/'
output_dir    = '../../data/' + taskname + '/tmp_deploy/'

deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'
hostname_filename    = 'hostnames.txt'
entrance_filename    = 'entrances.txt'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

IF_DATA_READ = 0

username = 'cuhk_cse_02'
projname = 'cdn_selection'
taskname = 'hostname_search'
hostname_prefix = 'hostnames'

wait_time = 20


hostnames = set()


def store_output_files():
  # print "Store!"
  hostnames.update(set(list_data.load_data(hosts_dir + hostname_filename)))
  list_data.store_data(hosts_dir + hostname_filename, list(hostnames))


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

if os.path.exists(plnode_dir + ready_node_filename):
  filename = plnode_dir + ready_node_filename
else:
  filename = plnode_dir + deploy_node_filename

#######
filename = plnode_dir + deploy_node_filename
#######

nodes = list_data.load_data(filename)
if DEBUG3: print "  %d nodes" % (len(nodes))


###################
## Get Entrance
###################
if DEBUG2: print "Get Entrance"

entrances = list_data.load_data(hosts_dir + entrance_filename)
if DEBUG3: print "  %d entrances" % (len(entrances))

IF_DATA_READ = 1


###################
## Generate Parameter Files
###################
if DEBUG2: print "Generate Parameter Files"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]
for ni in xrange(0,len(nodes)):
  node = nodes[ni]

  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  os.system("cp %s%s %s.%s" % (hosts_dir, entrance_filename, PARAM_FILE, node))


###################
## run jobs
###################
if DEBUG2: print "Run Jobs"

## Copy parameter to the server
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s.{} %s@{}:~/%s/git_repository/%s/%s" % (filename, output_dir, wait_time, PARAM_FILE, username, projname, taskname, PARAM_FILE))

## Execute
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d -P 100 ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"cd %s/git_repository/%s; rm %s; nohup python %s.py &> %s &\" " % (filename, output_dir, wait_time, username, projname, taskname, NOHUP_LOG_FILE, progname, NOHUP_LOG_FILE))


## rm parameters
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  os.system("rm %s.%s" % (PARAM_FILE, node))


