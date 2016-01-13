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
taskname = 'dns_cname_search'
progname = 'dns_cname_search'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
hostname_dir  = '../../data/hostname_search/hostname/'
cname_dir     = '../../data/' + taskname + '/dns_cname/'
output_dir    = '../../data/' + taskname + '/tmp_run/'

hostname_filename    = 'hostnames.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

cname_dict_filename      = 'cnames.data'
cname_filename           = 'cnames.txt'
auth_dict_filename       = 'auth_dns.data'
auth_filename            = 'auth_dns.txt'
fail_cname_dict_filename = 'cnames.fail.data'
fail_cname_filename      = 'cnames.fail.txt'
fail_auth_dict_filename  = 'auth_dns.fail.data'
fail_auth_filename       = 'auth_dns.fail.txt'

IF_DATA_READ = 0

wait_time = 100


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
## get data
###################
if DEBUG2: print "Get Data"

cnames = data.load_data(cname_dir + cname_dict_filename)
auths  = data.load_data(cname_dir + auth_dict_filename)
cname_list = set(list_data.load_data(cname_dir + cname_filename))
auth_list  = set(list_data.load_data(cname_dir + auth_filename))
fail_cnames = data.load_data(cname_dir + fail_cname_dict_filename)
fail_auths  = data.load_data(cname_dir + fail_auth_dict_filename)
fail_cname_list = set(list_data.load_data(cname_dir + fail_cname_filename))
fail_auth_list  = set(list_data.load_data(cname_dir + fail_auth_filename))


###################
## find remain hostnames
###################
if DEBUG2: print "Find Remain Hostnames"

hostnames = set(list_data.load_data(hostname_dir + hostname_filename))
if DEBUG3: print "  #hostnames=%d" % (len(list(hostnames)))
done_hostnames = set(cnames.keys())
if DEBUG3: print "  #done hostnames=%d" % (len(list(done_hostnames)))
remain_hostnames = hostnames - done_hostnames
hostnames = list(remain_hostnames)
if DEBUG3: print "  #remaining hostnames=%d" % (len(list(hostnames)))


###################
## Arrange Jobs
###################
if DEBUG2: print "Arrange Jobs"

njobs = math.ceil(len(hostnames) * 1.0 / len(nodes))
if DEBUG3: print "  %d jobs per node" % (njobs)
# exit()


###################
## Generate Parameter Files
###################
if DEBUG2: print "Generate Parameter Files"

# nodes = ["planetlab1.ie.cuhk.edu.hk"]
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  std = int(ni * njobs)
  end = int( min((ni+1)*njobs-1, len(hostnames)-1) )
  if DEBUG3: print("------------------------\n  %d/%d: %s [%d-%d]" % (ni+1, len(nodes), node, std, end))

  while std >= len(hostnames):
    # if DEBUG3: print("  no more hostnames")
    # break;
    std = int(std - len(hostnames))
    end = int( min(std+njobs-1, len(hostnames)-1) )
  
  
  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  fp = open(PARAM_FILE + "." + node, 'w')
  for idx in xrange(std, end+1):
    fp.write("%s\n" % (hostnames[idx]));
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
os.system("python vxargs.py -a %s -o %s -t %d ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"cd %s/git_repository/%s; rm %s; nohup python dns_cname_search.py &> %s &\" " % (filename, output_dir, wait_time, username, projname, taskname, NOHUP_LOG_FILE, NOHUP_LOG_FILE))

## rm parameters
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  os.system("rm %s.%s" % (PARAM_FILE, node))


