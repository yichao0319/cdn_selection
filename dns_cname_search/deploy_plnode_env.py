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
taskname = 'dns_cname_search'

plnode_dir  = '../../data/' + taskname + '/plnode/'
output_dir = '../../data/' + taskname + '/tmp_deploy/'
node_filename = 'nodes-current.txt'
deploy_node_filename = 'nodes-current-deploy.txt'
abnormal_filename = 'abnormal_list'
killed_filename   = 'killed_list'




###################
## Main
###################

####################
## select the init list
####################
# init_node_filename = node_filename
init_node_filename = deploy_node_filename


###################
## check PlanetLab nodes states
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Check PlanetLab Node states"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 5 ssh -oBatchMode=yes -i ~/.ssh/planetlab_rsa %s@{}  -oStrictHostKeyChecking=no \"hostname\"" % (plnode_dir, init_node_filename, output_dir, username))



###################
## find alive nodes
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Find Alive Nodes"

dead_nodes = set(load_data(output_dir + abnormal_filename))
dead_nodes |= set(load_data(output_dir + killed_filename))
# print str(dead_nodes)
current_nodes = set(load_data(plnode_dir + node_filename))
# print str(current_nodes)
remain_nodes = list(current_nodes - dead_nodes)
# print str(remain_nodes)
store_data(plnode_dir + deploy_node_filename, remain_nodes)


###################
## Install Python 2.7
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Install Python 2.7"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 2000 ssh -oBatchMode=yes -i ~/.ssh/planetlab_rsa %s@{}  -oStrictHostKeyChecking=no \"if [ -a Python-2.7.10 ] ; then exit; fi; sudo yum -y --nogpgcheck groupinstall 'Development tools'; sudo yum -y --nogpgcheck install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel; mkdir ~/python; wget --no-check-certificate https://www.python.org/ftp/python/2.7.10/Python-2.7.10.tgz; tar zxfv Python-2.7.10.tgz; rm Python-2.7.10.tgz; find ~/python -type d | xargs chmod 0755; cd Python-2.7.10; ./configure --prefix=/home/%s/python; make; sudo make install; echo 'export PATH=/home/%s/python/bin/:\$PATH' >> ~/.bashrc; source ~/.bashrc; wget --no-check-certificate  https://bootstrap.pypa.io/get-pip.py; sudo python get-pip.py; \"" % (plnode_dir, deploy_node_filename, output_dir, username, username, username))


###################
## check alive nodes again
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Check Alive Nodes again"

dead_nodes = set(load_data(output_dir + abnormal_filename))
dead_nodes |= set(load_data(output_dir + killed_filename))
# print str(dead_nodes)
current_nodes = set(load_data(plnode_dir + deploy_node_filename))
# print str(current_nodes)
remain_nodes = list(current_nodes - dead_nodes)
# print str(remain_nodes)
store_data(plnode_dir + deploy_node_filename, remain_nodes)

if DEBUG3: print "  #alive nodes=%d" % (len(remain_nodes))


###################
## Install Other Package
###################
if DEBUG2: print "\n========================="
if DEBUG2: print "Install Other Package"

os.system("rm -rf %s" % (output_dir))
os.system("python vxargs.py -a %s%s -o %s -t 2000 ssh -oBatchMode=yes -i ~/.ssh/planetlab_rsa %s@{}  -oStrictHostKeyChecking=no \"sudo yum -y --nogpgcheck install python-setuptools; python -m pip install dnspython; sudo python -m pip install dnspython\"" % (plnode_dir, deploy_node_filename, output_dir, username))
