#!/usr/bin/python
import sys, os
from list_data import *


username = 'cuhk_cse_02'
# node_filename  = '../../data/hostname_search/plnode/nodes-current-deploy.txt'
node_filename  = '../../data/hostname_search/plnode/nodes-current.txt'
output_dir = '../../data/hostname_search/tmp_deploy/'

os.system("rm -rf %s" % (output_dir))
# os.system("python vxargs.py -a %s -o %s -t 30 -P 100 ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"ps awx | grep parse_links.py | grep -v 'grep' | awk '{print \$1}' | xargs kill -9\"" % (node_filename, output_dir, username))
# os.system("python vxargs.py -a %s -o %s -t 30 -P 100 ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"ps awx | grep python | grep -v 'grep' | awk '{print \$1}' | xargs kill -9\"" % (node_filename, output_dir, username))
os.system("python vxargs.py -a %s -o %s -t 30 -P 100 ssh -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa %s@{} \"rm -rf ./cdn_selection/data/hostname_search/hostname/hostnames.txt; rm -rf cdn_selection/git_repository/hostname_search/tmp.nohup.parse_links.out\"" % (node_filename, output_dir, username))
