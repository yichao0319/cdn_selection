#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, os, math, re, fnmatch, signal, time, locale
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
wait_time = 90

hostnames = set()


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
force_utf8_hack()


###################
## get PlanetLab nodes states
###################
if DEBUG2: print "Get PlanetLab Nodes"

nodes = list_data.load_data(plnode_dir + deploy_node_filename)
if DEBUG3: print "  %d nodes" % (len(nodes))


###################
## Get Hostname
###################
if DEBUG2: print "Get Existing Hostname"

hostnames = set(list_data.load_data(hosts_dir + hostname_filename))
if DEBUG3: print "  %d hostnames" % (len(list(hostnames)))

IF_DATA_READ = 1


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
os.system("python vxargs.py -a %s%s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/data/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, dirname))
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

  ## get existing hostnames
  if os.path.exists("%s/%s/" % (tmp_dir, dirname)):
    if DEBUG3: print "    Update hostnames: %d" % (len(list(hostnames)))
    new_hostnames = set(list_data.load_data("%s/%s/%s" % (tmp_dir, dirname, hostname_filename)))
    hostnames.update(new_hostnames)
    if DEBUG3: print "               after: %d" % (len(list(hostnames)))

  else:
    continue

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
  print "  # Crawlled Hostnames: %d" % (len(list(hostnames)))
  # print "    "+"\n    ".join(list(hostnames))

## Update local files
store_output_files()
list_data.store_data(plnode_dir + ready_node_filename, nodes_ready)


