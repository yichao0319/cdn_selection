#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, os, math, re, fnmatch, signal, time, locale
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
dirname  = 'dns_cname'

DONE_IND_FILE    = 'tmp.' + progname + '.done'
KILLED_IND_FILE  = 'tmp.' + progname + '.killed'
RUNNING_IND_FILE = 'tmp.' + progname + '.running'
NOHUP_LOG_FILE   = 'tmp.nohup.' + progname + '.out'
PARAM_FILE       = 'tmp.' + progname + '.param.txt'

plnode_dir    = '../../data/' + taskname + '/plnode/'
hostname_dir  = '../../data/hostname_search/hostname/'
cname_dir     = '../../data/' + taskname + '/' + dirname + '/'
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
  data.store_data(cname_dir + cname_dict_filename, cnames)
  data.store_data(cname_dir + auth_dict_filename, auths)
  list_data.store_data(cname_dir + cname_filename, list(cname_list))
  list_data.store_data(cname_dir + auth_filename, list(auth_list))
  data.store_data(cname_dir + fail_cname_dict_filename, fail_cnames)
  data.store_data(cname_dir + fail_auth_dict_filename, fail_auths)
  list_data.store_data(cname_dir + fail_cname_filename, list(fail_cname_list))
  list_data.store_data(cname_dir + fail_auth_filename, list(fail_auth_list))


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
os.system("python vxargs.py -a %s%s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/data/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, dirname))
## job done indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, DONE_IND_FILE))
## job running indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, RUNNING_IND_FILE))
## job killed indicator
os.system("rm -rf " + output_dir)
os.system("python vxargs.py -a %s%s -o %s -t %d -P 100 scp -oBatchMode=yes -oStrictHostKeyChecking=no -i ~/.ssh/planetlab_rsa -r %s@{}:~/%s/git_repository/%s/%s ./tmp.{}/" % (plnode_dir, deploy_node_filename, output_dir, wait_time, username, projname, taskname, KILLED_IND_FILE))
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

  ## read files
  this_cnames = data.load_data(tmp_dir + "/" + dirname + "/" + cname_dict_filename)
  this_auths  = data.load_data(tmp_dir + "/" + dirname + "/" + auth_dict_filename)
  this_cname_list = set(list_data.load_data(tmp_dir + "/" + dirname + "/" + cname_filename))
  this_auth_list  = set(list_data.load_data(tmp_dir + "/" + dirname + "/" + auth_filename))
  this_fail_cnames = data.load_data(tmp_dir + "/" + dirname + "/" + fail_cname_dict_filename)
  this_fail_auths  = data.load_data(tmp_dir + "/" + dirname + "/" + fail_auth_dict_filename)
  this_fail_cname_list = set(list_data.load_data(tmp_dir + "/" + dirname + "/" + fail_cname_filename))
  this_fail_auth_list  = set(list_data.load_data(tmp_dir + "/" + dirname + "/" + fail_auth_filename))

  ## add to cnames
  for hostname in this_cnames:
    if hostname in cnames:
      cnames[hostname].update(this_cnames[hostname])
    else:
      cnames[hostname] = this_cnames[hostname]

  cname_list.update(this_cname_list)

  ## add to done auths
  for hostname in this_auths:
    if hostname in auths:
      auths[hostname].update(this_auths[hostname])
    else:
      auths[hostname] = this_auths[hostname]

  auth_list.update(this_auth_list)

  print "    # CNAMEs: %d (+%d)" % (len(cnames), len(this_cnames))
  print "    # DNSes:  %d (+%d)" % (len(auths), len(this_auths))

  ## add to failed cnames
  for hostname in this_fail_cnames:
    if hostname in fail_cnames:
      fail_cnames[hostname].update(this_fail_cnames[hostname])
    else:
      fail_cnames[hostname] = this_fail_cnames[hostname]

  fail_cname_list.update(this_fail_cname_list)

  ## add to failed auths
  for hostname in this_fail_auths:
    if hostname in fail_auths:
      fail_auths[hostname].update(this_fail_auths[hostname])
    else:
      fail_auths[hostname] = this_fail_auths[hostname]

  fail_auth_list.update(this_fail_auth_list)

  ## remove tmp_dir
  os.system("rm -rf %s" % (tmp_dir))
  time.sleep(0.1)


## Update local files
store_output_files()
if len(nodes_ready) > 0:
  list_data.store_data(plnode_dir + ready_node_filename, nodes_ready)


if DEBUG3:
  print "  Ready Nodes: "
  print "    "+"\n    ".join(nodes_ready)
  print "  Running Nodes: "
  print "    "+"\n    ".join(nodes_running)
  print "  Bad Nodes: "
  print "    "+"\n    ".join(nodes_bad)
  print "  # Hostnames: %d" % (len(cnames))
  print "  # CNAMEs: %d" % (len(cname_list))
  print "  # Domains:  %d" % (len(auths))
  print "  # DNSes:  %d" % (len(auth_list))

