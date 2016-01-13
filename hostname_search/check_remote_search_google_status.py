#!/usr/bin/python
import sys, os, math, re, fnmatch, signal, time
from list_data import *


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1


###################
## Variables
###################
plnode_dir    = '../../data/hostname_search/plnode/'
hostname_dir  = '../../data/hostname_search/hostname/'
output_dir    = '../../data/hostname_search/tmp_deploy/'
deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'
keyword_filename     = 'keyword_list.txt'
done_filename        = 'keyword_list_done.txt'

DONE_IND_FILE    = 'tmp.search_google.done'
KILLED_IND_FILE  = 'tmp.search_google.killed'
RUNNING_IND_FILE = 'tmp.search_google.running'
NOHUP_LOG_FILE   = 'tmp.nohup.search_google.out'
PARAM_FILE       = 'tmp.search_google.param.txt'

IF_HOST_READ = 0

username = 'cuhk_cse_02'
projname = 'cdn_selection'
taskname = 'hostname_search'
hostname_prefix = 'hostnames'

wait_time = 100


def read_hostname_files(path, prefix):
  hostnames = set()
  for fn in os.listdir(path):
    # print "  "+path+fn
    if fnmatch.fnmatch(fn, prefix+'*'):
      # print "  "+path+fn
      hostnames |= set(load_data(path+fn))
      
  return hostnames


# handle crl+c event
def signal_handler(signal, frame):
  if DEBUG2: print 'You pressed Ctrl+C!'
  if IF_HOST_READ:
    store_data(("%s%s.txt" % (hostname_dir, hostname_prefix)), list(hostnames))
    store_data(("%s%s" % (hostname_dir, done_filename)), list(done_keywords))
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

nodes = load_data(plnode_dir + deploy_node_filename)
## remove cn nodes for Google search
new_nodes = []
for node in nodes:
  m = re.match(".*\.cn", node)
  if m is None:
    new_nodes.append(node)
nodes = new_nodes
if DEBUG3: print "  %d nodes" % (len(nodes))


###################
## get Keywords and Hostnames
###################
if DEBUG2: print "Get Keywords and Hostnames"

keywords = set(load_data(hostname_dir + keyword_filename))
if DEBUG3: print "  %d keywords" % (len(list(keywords)))

done_keywords = set(load_data(hostname_dir + done_filename))
if DEBUG3: print "  %d done keywords" % (len(list(done_keywords)))

hostnames = set(read_hostname_files(("%s" % (hostname_dir)), hostname_prefix))
if DEBUG3: print "  %d hostnames" % (len(list(hostnames)))
IF_HOST_READ = 1


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
os.system("python vxargs.py -a %s%s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/data/%s/hostname ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname))
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

  # ## output files
  # signal.alarm(wait_time)
  # try:
  #   os.system("scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@%s:~/%s/data/%s/hostname ./%s/" % (username, node, projname, taskname, tmp_dir))
  #   ## job done indicator
  #   os.system("scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@%s:~/%s/git_repository/%s/%s ./%s/" % (username, node, projname, taskname, DONE_IND_FILE, tmp_dir))
  #   ## job running indicator
  #   os.system("scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@%s:~/%s/git_repository/%s/%s ./%s/" % (username, node, projname, taskname, RUNNING_IND_FILE, tmp_dir))
  #   ## job killed indicator
  #   os.system("scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@%s:~/%s/git_repository/%s/%s ./%s/" % (username, node, projname, taskname, KILLED_IND_FILE, tmp_dir))
  # except Exception, exc:
  #   print exc
  #   nodes_bad.append(node)
  #   continue

  # signal.alarm(0)


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

  ## read done keywords
  if DEBUG3: print "    Update keywords: %d" % (len(list(done_keywords)))
  done_keywords.update(set(load_data("%s/hostname/%s" % (tmp_dir, done_filename))))
  ## get existing hostnames
  if os.path.exists(("%s/hostname/" % (tmp_dir))):
    if DEBUG3: print "    Update hostnames: %d" % (len(list(hostnames)))
    hostnames.update(set(read_hostname_files(("%s/hostname/" % (tmp_dir)), hostname_prefix)))
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
  print "  Crawlled Keywords: "
  print "    "+"\n    ".join(list(done_keywords))
  print "  # Crawlled Hostnames: %s" % (len(list(hostnames)))
  # print "    "+"\n    ".join(list(hostnames))

## Update local files
store_data(("%s%s.txt" % (hostname_dir, hostname_prefix)), list(hostnames))
store_data(("%s%s" % (hostname_dir, done_filename)), list(done_keywords))
store_data(plnode_dir + ready_node_filename, nodes_ready)


