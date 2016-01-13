#!/usr/bin/python
import sys, os
from list_data import *


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

plnode_dir = '../../data/' + taskname + '/plnode/'
output_dir = '../../data/' + taskname + '/tmp_deploy/'

# node_filename        = 'nodes-current.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
abnormal_filename    = 'abnormal_list'
killed_filename      = 'killed_list'



###################
## Main
###################

###################
## make directory for data
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Make Directory for Data"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 10 ssh -oBatchMode=yes -i ~/.ssh/planetlab_rsa %s@{}  -oStrictHostKeyChecking=no \"mkdir -p ~/%s/data/%s/ips; mkdir -p ~/%s/data/%s/plnode; mkdir -p ~/%s/git_repository\"" % (plnode_dir, deploy_node_filename, output_dir, username, projname, taskname, projname, taskname, projname))


###################
## find alive nodes
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Find Alive Nodes (1)"

dead_nodes = set(load_data(output_dir + abnormal_filename))
dead_nodes |= set(load_data(output_dir + killed_filename))
# print str(dead_nodes)
current_nodes = set(load_data(plnode_dir + deploy_node_filename))
# print str(current_nodes)
remain_nodes = list(current_nodes - dead_nodes)
# print str(remain_nodes)
store_data(plnode_dir + deploy_node_filename, remain_nodes)



###################
## delete old codes
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Delete Old Codes"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 30 ssh -oBatchMode=yes -i ~/.ssh/planetlab_rsa %s@{}  -oStrictHostKeyChecking=no \"rm -rf ~/%s/git_repository/%s\"" % (plnode_dir, deploy_node_filename, output_dir, username, projname, taskname))


###################
## copy new codes
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Copy New Codes"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 30 scp -oBatchMode=yes -i ~/.ssh/planetlab_rsa -r ../%s %s@{}:~/%s/git_repository/%s" % (plnode_dir, deploy_node_filename, output_dir, taskname, username, projname, taskname))


###################
## check alive nodes again
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Check Alive Nodes again (3)"

dead_nodes = set(load_data(output_dir + abnormal_filename))
dead_nodes |= set(load_data(output_dir + killed_filename))
# print str(dead_nodes)
current_nodes = set(load_data(plnode_dir + deploy_node_filename))
# print str(current_nodes)
remain_nodes = list(current_nodes - dead_nodes)
# print str(remain_nodes)
store_data(plnode_dir + deploy_node_filename, remain_nodes)

## remove output dir
os.system("rm -rf " + output_dir)
