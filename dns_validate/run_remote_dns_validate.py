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
## read dns candidate
###################
if DEBUG2: print "Read DNS Candidate"

auths  = set(list_data.load_data(cname_dir + auth_filename))

if DEBUG3: print "  #auth dns=%d" % (len(auths))


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
## find remain DNS
###################
if DEBUG2: print "Find Remain DNS"

remain_auths = auths - valid_dns
remain_auths = remain_auths - invalid_dns
auths = list(remain_auths)


###################
## Arrange Jobs
###################
if DEBUG2: print "Arrange Jobs"

njobs = math.ceil(len(auths) * 1.0 / len(nodes))
if DEBUG3: print "  %d jobs per node" % (njobs)
# exit()


###################
## Generate Parameter Files
###################
if DEBUG2: print "Generate Parameter Files"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]

## assigned candidate DNS
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  std = int(ni * njobs)
  end = int( min((ni+1)*njobs-1, len(auths)-1) )
  if DEBUG3: print("------------------------\n  %d/%d: %s [%d-%d]" % (ni+1, len(nodes), node, std, end))

  while std >= len(auths):
    # if DEBUG3: print("  no more auths")
    # break;
    std = int(std - len(auths))
    end = int( min(std+njobs-1, len(auths)-1) )
  
  
  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  fp = open(PARAM_FILE + "." + node, 'w')
  for idx in xrange(std, end+1):
    fp.write("%s\n" % (auths[idx]));
  fp.close()


###################
## run jobs
###################
if DEBUG2: print "Run Jobs"

## Copy parameter to the server
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s.{} %s@{}:~/%s/git_repository/%s/%s" % (filename, output_dir, wait_time, PARAM_FILE, username, projname, taskname, PARAM_FILE))

## Execute
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"cd %s/git_repository/%s; rm %s; nohup python dns_validate.py &> %s &\" " % (filename, output_dir, wait_time, username, projname, taskname, NOHUP_LOG_FILE, NOHUP_LOG_FILE))

## rm parameters
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  os.system("rm %s.%s" % (PARAM_FILE, node))


