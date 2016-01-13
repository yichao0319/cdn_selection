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
output_dir    = '../../data/hostname_search/tmp_run/'
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

if os.path.exists(plnode_dir + ready_node_filename): 
  filename = plnode_dir + ready_node_filename
else:
  filename = plnode_dir + deploy_node_filename

nodes = load_data(filename)
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


remain_keywords = keywords - done_keywords
keywords = list(remain_keywords)
# exit();


###################
## Arrange Jobs
###################
if DEBUG2: print "Arrange Jobs"

njobs = math.ceil(len(keywords) * 1.0 / len(nodes))
if DEBUG3: print "  %d jobs per node" % (njobs)
# exit()


###################
## run jobs
###################
if DEBUG2: print "Run Jobs"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  std = int(ni * njobs)
  end = int( min((ni+1)*njobs-1, len(keywords)-1) )
  if DEBUG3: print("------------------------\n  %d/%d: %s [%d-%d]" % (ni+1, len(nodes), node, std, end))

  if std >= len(keywords):
    if DEBUG3: print("  no more keywords")
    break;
  
  
  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  fp = open("%s" % (PARAM_FILE), 'w')
  for idx in xrange(std, end+1):
    fp.write("%s\n" % (keywords[idx]));
  fp.close()

  ## Copy parameter to the server
  if DEBUG3: print("  Copy parameter to the server")
  os.system("scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s %s@%s:~/%s/git_repository/%s/ " % (PARAM_FILE, username, node, projname, taskname))

  ## Execute
  os.system("ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@%s \"cd %s/git_repository/%s; rm %s; nohup python search_google.py &> %s &\"" %(username, node, projname, taskname, NOHUP_LOG_FILE, NOHUP_LOG_FILE))




