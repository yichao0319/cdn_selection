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
taskname = 'ip_geolocate'
progname = 'ipinfo'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
geo_db_dir    = '../../data/' + taskname + '/database/'
ips_dir       = '../../data/ip_search/ips/'
output_dir    = '../../data/' + taskname + '/tmp_run/'

deploy_node_filename = 'nodes-current-deploy.txt'
ready_node_filename  = 'nodes-current-deploy-ready.txt'

ip_dict_filename     = 'ips.data'
ipinfo_db_filename   = 'ipinfo_db.data'

IF_DATA_READ = 0

time_cnt = 0
wait_time = 10

located_ips = {}


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
## Read IP to Geolocation Database
###################
if DEBUG2: print "Read IP to Geolocation Database"

located_ips = data.load_data(geo_db_dir + ipinfo_db_filename)
IF_DATA_READ = 1

if DEBUG3: print "  #records=%d" % (len(located_ips))


###################
## read ips
###################
if DEBUG2: print "Read IPs"

ips = data.load_data(ips_dir + ip_dict_filename)
remain_ips = set([])
for cname in ips:
  for dns in ips[cname]:
    for ip in ips[cname][dns]:
      if ip not in located_ips:
        remain_ips.add(ip)

remain_ips = list(remain_ips)
if DEBUG3: print "  #remainning ips: %d" % (len(remain_ips))


###################
## Arrange Jobs
###################
if DEBUG2: print "Arrange Jobs"

njobs = math.ceil(len(remain_ips) * 1.0 / len(nodes))
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
  end = int( min((ni+1)*njobs-1, len(remain_ips)-1) )
  if DEBUG3: print("------------------------\n  %d/%d: %s [%d-%d]" % (ni+1, len(nodes), node, std, end))

  while std >= len(remain_ips):
    # if DEBUG3: print("  no more remain_ips")
    # break;
    std = int(std - len(remain_ips))
    end = int( min(std+njobs-1, len(remain_ips)-1) )


  ## Write parameter file
  if DEBUG3: print("  Write parameter file")
  fp = open(PARAM_FILE + "." + node, 'w')
  for idx in xrange(std, end+1):
    fp.write("%s\n" % (remain_ips[idx]));
  fp.close()


###################
## run jobs
###################
if DEBUG2: print "Run Jobs"

## Copy parameter to the server
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s.{} %s@{}:~/%s/git_repository/%s/%s" % (filename, output_dir, wait_time, PARAM_FILE, username, projname, taskname, PARAM_FILE))

# exit()
## Execute
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s -o %s -t %d ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"cd %s/git_repository/%s; rm %s; nohup python ipinfo.py &> %s &\" " % (filename, output_dir, wait_time, username, projname, taskname, NOHUP_LOG_FILE, NOHUP_LOG_FILE))


## rm parameters
for ni in xrange(0,len(nodes)):
  node = nodes[ni]
  os.system("rm %s.%s" % (PARAM_FILE, node))


