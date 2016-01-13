#!/usr/bin/python
import sys, os, urllib, xmlrpclib, socket, sets


## static variables
DEBUG1 = 1
DEBUG2 = 1
DEBUG3 = 1

###################
## Main
###################
remove_nbnodes = '-r' in sys.argv
no_file_writes = '-n' in sys.argv
os.system('rm tmp.update_slide_node.done')


###################
## Variables
###################
input_dir  = '../../data/hostname_search/plnode/'
output_dir = '../../data/hostname_search/plnode/'


###################
## the PL Central API
###################
if DEBUG2: print "Access PLCAPI"

apiurl = 'https://www.planet-lab.org/PLCAPI/'
api_server = xmlrpclib.ServerProxy(apiurl, allow_none=True)

## the auth struct
auth = {}
auth['Role'] = "user"
auth['AuthMethod'] = "password"
auth['Username'] = 'wjwu@cse.cuhk.edu.hk'
auth['AuthString'] = 'fntlfntl'
slice_name = 'cuhk_cse_02'

authorized = api_server.AuthCheck(auth)
if authorized:
  print '  We are authorized!'
else:
  print "  [FAILURE] Permission denied."
  sys.exit()


###################
## get the all the nodes, regardless of current boot state
###################
if DEBUG2: print "Retrieve Node Lists"

sys.stdout.flush()
all_nodes = api_server.GetNodes(auth, {}, ['node_id', 'hostname', 'boot_state'])
# all_nodes = api_server.GetNodes(auth)
# print all_nodes
have_node_ids = api_server.GetSlices(auth, [ slice_name ], ['node_ids'])[0]['node_ids']
have_nodes = [node['hostname'] for node in api_server.GetNodes(auth, have_node_ids, ['hostname'])]


###################
# nodes is an array of associative arrays/dictionary objects, each one
# corresponding to a node returned. the keys in each array element are
# the keys specified in return_fields
###################
if DEBUG2: print "Update Slice"

toadd_nodes = []
todel_nodes = []

for node_record in all_nodes:
  # if (node_record['hostname'] not in have_nodes) and node_record['boot_state'] == 'boot':
  if (node_record['hostname'] not in have_nodes):
    toadd_nodes.append(node_record['hostname'])
  elif (node_record['hostname'] in have_nodes) and node_record['boot_state'] != 'boot':
    todel_nodes.append(node_record['hostname'])

print "  Found %d new node(s)" % (len(toadd_nodes))
print "  Found %d node(s) no longer in boot state" % (len(todel_nodes))

###################
## add node to slice
###################
if len(toadd_nodes) > 0:
  if DEBUG2: print "Adding new nodes to slice"

  result = api_server.AddSliceToNodes(auth, slice_name, toadd_nodes)
  if result == 1:
    print "  SUCCESS"
  else:
    print "  FAILED!"
  sys.stdout.flush()

  fp = open(output_dir+'nodes-new.txt','w')
  for node in toadd_nodes:
    fp.write("%s\n" % (node));
  fp.close()


###################
## remove nodes from slice
###################
if remove_nbnodes and len(todel_nodes) > 0:
  
  if DEBUG2: print "Removing non-boot nodes from slice"

  result = api_server.SliceNodesDel(auth, slice_name, todel_nodes)
  if result == 1:
    print "  SUCCESS"
  else:
    print "  FALIED!"
  sys.stdout.flush()


###################
## write out all node list
###################
if not no_file_writes:
  if DEBUG2: print "Writing out all node list"

  afp = open(output_dir+'nodes-all.txt','w')
  for node in all_nodes:
    afp.write("%s\n" % (node))
  afp.close()
  print "done."

  # write out current node list
  print "Writing out current node list ...",
  sys.stdout.flush()

  have_node_ids = api_server.GetSlices(auth, [ slice_name ], ['node_ids'])[0]['node_ids']
  have_nodes = [node['hostname'] for node in api_server.GetNodes(auth, have_node_ids, ['hostname'])]
  
  cfp = open(output_dir+'nodes-current.txt','w')
  for node in have_nodes:
    cfp.write("%s\n" % (node))
  cfp.close()
  
  ## write out current alive node list
  cfp = open(output_dir+'nodes-current-alive.txt','w')
  for node_record in all_nodes:
    if (node_record['hostname'] in have_nodes) and node_record['boot_state'] == 'boot':
      cfp.write("%s\n" % (node_record['hostname']))
  cfp.close()
  print "done."


os.system('touch tmp.update_slide_node.done')
