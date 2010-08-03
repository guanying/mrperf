#!/usr/bin/python


"""Hadoop Simulator

This simulator takes three configuration files, topology.xml, metadata.xml,
and job.xml, describing a Hadoop job and the topology it will run on.
Two tcl files, topology.tcl and events.tcl, will be generated as input
for ns-2 for further simulation.
"""

#import xml.dom
import xml.dom.minidom
import sys
from optparse import OptionParser
import random
from gen import *
#import getopt

def convert(topo_xml):
	root = xml.dom.minidom.parse(topo_xml)
	#topo = root.getElementsByTagName(u"topo")[0]
	topo = xml_children(root, u'topo')[0]
	racks = {}
	#lines = ["proc create-topology { } {\n", "\tglobal ns opt\n"]
	for rack_group in topo.getElementsByTagName(u"rack_group"):
		numrack = len(rack_group.getElementsByTagName(u"rack_index"))
		namenode = rack_group.getElementsByTagName(u'name')[0]
		name = str(namenode.childNodes[0].nodeValue)
		node_group = rack_group.getElementsByTagName(u'compute_node_group')[0]
		numnode = len(node_group.getElementsByTagName(u'node_index'))
		numswitch = len(rack_group.getElementsByTagName(u'switch_index'))
		#lines.append("\tglobal %s\n" % (" ".join([name+'_'+str(i) for i in range(numrack)])))
		lines.append("\tfor {set i 0} {$i < %d} {incr i} {\n" % (numrack))
		lines.append("\t\tcreate-nodes %s_$i %d\n" % (name, numnode))
		lines.append("\t}\n")
		if name in racks.keys():
			print "error: rack_group name \"%s\" conflict\n" % (name)
		connect = [[0]*numswitch for i in range(numrack)]
		racks[name] = [name, numrack, numnode, numswitch, connect]

	for router in topo.getElementsByTagName(u'router'):
		router_name = str(router.getElementsByTagName(u'name')[0].childNodes[0].nodeValue)
		lines.append("\tset %s [$ns node]\n" % (router_name))
		for group in router.getElementsByTagName(u'connect_to_group'):
			switch_index = int(group.getElementsByTagName(u'switch_index')[0].childNodes[0].nodeValue)
			rgname = str(group.getElementsByTagName(u'rack_group_name')[0].childNodes[0].nodeValue)
			if rgname not in racks.keys():
				print "error: rack group name %s not defined\n" % (rgname)
			p = racks[rgname]
			numrack = p[1]
			numnode = p[2]
			numswitch = p[3]
			connect = p[4]
			for i in range(numrack):
				rack = connect[i]
				if rack[switch_index] <> 0:
					print "error: a switch (rack %s[%d] switch %d = %s) connected to multiple routers\n" % (rgname, i, switch_index, repr(rack[switch_index]))
				rack[switch_index] = 1 #to indicate it's already written to tcl

			#LAN with router
			lines.append("\tfor {set i 0} {$i < %s} {incr i} {\n" % (numrack))
			lines.append("\t\tcreate-lan $%s %s_$i %d\n" % (router_name, rgname, numnode))
			lines.append("\t}\n")

		for connect_to in router.getElementsByTagName(u'connect_to'):
			print "hello"

	lines.append('}\n')

	f = open("hadoop.topo.tcl", "w")
	f.writelines(lines)
	f.close()
	#print lines

def main():
	usage = "usage: %prog options"
	parser = OptionParser(usage)
	parser.add_option("-v", "--verbose", default=False,
					action="store_true", dest="verbose")
	parser.add_option("-t", "--topology", dest="topo_xml",
					help="topology configuration xml")
	parser.add_option("-m", "--metadata", dest="meta_xml", 
					help="metadata configuration xml")
	parser.add_option("-j", "--job", dest="job_xml", 
					help="job configuration xml")
	parser.add_option("-T", "--topoout", dest="topo_tcl", 
					help="output tcl file describing topology",
					default="hadoop.topo.tcl")
	parser.add_option("-J", "--jobout", dest="job_tcl", 
					help="output tcl file describing job",
					default="hadoop.job.tcl")
	(options, args) = parser.parse_args()

	if None in (options.topo_xml, options.meta_xml, options.job_xml):
		print 'xmls not defined'
		parser.print_help()
		sys.exit()

	topo = topology_t(options.topo_xml)
	job = job_t(options.job_xml)

	topo.totcl(options.topo_tcl)
	#topo.totcl2('mapnodes.tcl')

'''
	f = open(options.job_tcl, 'w')
	f.write(job.tcl)
	f.close()
'''

if __name__ == "__main__":
	main()



