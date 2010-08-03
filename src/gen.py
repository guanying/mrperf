#!/usr/bin/python


import xml.dom.minidom
import sys
from optparse import OptionParser
import random
import math

from hadoop_conf import *

chunk_size = []

def xml_children(node, children_name):
	"""return list of node's children nodes with name of children_name"""
	return node.getElementsByTagName(children_name)

def xml_text(node):
	return node.childNodes[0].nodeValue

def xml_child_text(node, child_name):
	"""probably encoded in utf-8, be careful."""
	return xml_text(xml_children(node, child_name)[0])


class empty_t:
	pass

class hnode_t:
	"""HDFS node for a HDFS tree.

5-level hierarchy: rack_group (multiple identical racks), rack, node_group
(multiple identical nodes), node, and disk.
disk should be initiated with a capacity. Other nodes' capacity are
calculated by summing up children's capacity."""
	def __init__(self, parent, capacity=None, num=1):
		self.parent = parent
		self._capacity = capacity
		self._num = num
		self._children = []
		self.used = 0
		self.end = None
		self.reserved = None
		if parent <> None:
			self.index_stack = parent.index_stack[:] + [len(parent.children())]
			parent.children().append(self)
			if parent._capacity <> None:
				parent._capacity = None
		else:
			self.index_stack = []

	def clone(self, parent=None):
		'''clone a node from self, and append it to parent's children'''
		if parent == None:
			parent = self.parent
		node = hnode_t(parent, self._capacity)
		node._children = []
		if self._children <> []:
			for child in self._children:
				#print self, self.parent, self._children
				child.clone(node)
				#node._children.append(child.clone(node))  ## wrong!!!
		node.used = 0
		node.reserved = self.reserved
		return node

	def capacity(self):
		if self._capacity <> None:
			return self._capacity
		else :
			assert self._children <> []
			self._capacity = 0
			for child in self._children:
				self._capacity += child.capacity()
			return self._capacity

	def children(self):
		return self._children;

	def add_chunk(self):
		if self.used >= self.capacity():
			print 'error: node full: ' + str(self.index_stack)
		self.used += chunk_size
		parent = self.parent
		if parent != None:
			parent.add_chunk()

	def name(self):
		if len(self.index_stack) == 5:   #disk
			return 'd_rg%d_%d_ng%d_%d_disk%d' % tuple(self.index_stack)
		elif len(self.index_stack) == 4:   #node
			return 'n_rg%d_%d_ng%d_%d' % tuple(self.index_stack)
		elif len(self.index_stack) == 3:   #node group template
			return 'n_rg%d_%d_ng%d' % tuple(self.index_stack)
		elif len(self.index_stack) == 2:   #rack
			return 'r_rg%d_%d' % tuple(self.index_stack)
		elif len(self.index_stack) == 1:   #rack_group
			return 'rg_rg%d' % tuple(self.index_stack)
		else:
			print 'error: request name for unknown node type. (' \
					+ self.index_stack + ')'

	def dump(self, level=0):
		if options.verbose == False:
			return
		print self.index_stack, self.used, self._capacity, len(self.children())
		node = self
		if node.children() <> []:
			for child in node.children():
				child.dump()

	def prev_node(self):
		if self.index_stack == []:
			return None

		myindex = self.index_stack[-1]
		if myindex == 0:
			return self.parent.prev_node()

		siblings = self.parent.children()
		return siblings[myindex-1]

	def global_end(self):
		'''global index at the end of a node'''
		if self.end <> None:
			return self.end

		# end should be previous node's end + self.capacity()
		prev = self.prev_node()
		if prev <> None:
			self.end = prev.global_end() + self.capacity()
		else:
			# Otherwise, this is a first node
			self.end = self.capacity()
		return self.end

	def choose_disk(self):
		'''when a node is chosen for replication, it needs to choose a disk to put the data.'''
		if self.used >= self.capacity():
			return None

		disk_id = random.randrange(len(self.children()))
		disk = self.children()[disk_id]
		if disk.used < disk.capacity():
			return disk
		else:
			return self.choose_disk()

class zipf:
	def __init__(self, alpha, N):
		self.c = 0.0
		for i in range(1,N+1):
			self.c += 1.0/math.pow(i, alpha)
		self.c = 1/self.c
		self.alpha = float(alpha)
		self.N = N

		self.partial_sum = [0]

	def next(self):
		u = random.random()

		sum = self.partial_sum[-1]
		count = len(self.partial_sum)
		if sum >= u:
			# possible further optimization: binary search instead of traversal
			# because self.partial_sum is ordered.
			for i in range(1, count):
				sum = self.partial_sum[i]
				if sum >= u:
					zipf_value = i
					break
		else:
			for i in range(count, self.N+1):
				sum += self.c/math.pow(i, self.alpha)
				self.partial_sum.append(sum)
				if sum >= u:
					zipf_value = i
					break

		assert zipf_value >= 1 and zipf_value <=self.N

		return zipf_value

def pareto(xm, k, N):
	pareto_value = N+1
	while pareto_value > N:
		u = random.random()
		pareto_value = math.pow((1-u), -1.0/k) * xm

	return pareto_value

def loguniform(l, u):
	return math.exp(random.uniform(math.log(l), math.log(u)))

class machine_type_t:
	def __init__(self, mt):
		disk = xml_children(mt, u'disk')[0]
		self.disk = empty_t()
		self.disk.type = str(xml_child_text(disk, u'type'))
		self.disk.capacity = int(xml_child_text(disk, u'capa'))*1000 # in byte
		self.disk.num = int(xml_child_text(disk, u'num'))

		cpu = xml_children(mt, u'cpu')[0]
		self.cpu = empty_t()
		self.cpu.type = str(xml_child_text(cpu, u'type'))
		self.cpu.cores = int(xml_child_text(cpu, u'number_of_cores'))
		self.cpu.num = int(xml_child_text(cpu, u'num'))

		mem = xml_children(mt, u'mem')[0]
		self.mem = empty_t()
		self.mem.type = str(xml_child_text(mem, u'type'))
		self.mem.capacity = str(xml_child_text(mem, u'capa')) # in MB

		# TODO: other parts of machine_type

class topology_t:
	def __init__(self, topo_xml):
		root = xml.dom.minidom.parse(topo_xml)
		self.htree = hnode_t(None)
		self.dmt = {} # dict of machine type
		topo = root.getElementsByTagName(u"topo")[0]

		# populate dict of machine type
		list_machine_type = topo.getElementsByTagName(u'machine_type')
		for mt_node in list_machine_type:
			name = str(xml_child_text(mt_node, u'name'))
			self.dmt[name] = machine_type_t(mt_node)

		# topology
		for rack_group in xml_children(topo, u"rack_group"):
			rg_node = hnode_t(self.htree)
			# rgname not in use currently. maybe a name-node map is needed.
			rg_node.rgname = str(xml_child_text(rack_group, u'name'))
			num_rack = len(xml_children(rack_group, u"rack_index"))
			self.racks = num_rack
			rack_node = hnode_t(rg_node)

			# populate the first rack_node
			for node_group in xml_children(rack_group, u"compute_node_group"):
				ng_node = hnode_t(rack_node)
				# machine type and disk
				mt_name = str(xml_child_text(node_group, u'machine_type_name'))
				mt = self.dmt[mt_name]
				ng_node.reserved = mt

				num_node = len(xml_children(node_group, u'node_index'))
				self.nodes = num_node
				node_node = hnode_t(ng_node)

				# populate the first node_node
				for i in range(mt.disk.num):
					disk_node = hnode_t(node_node, mt.disk.capacity)

				#self.htree.dump()

				# clone other node_nodes
				for i in range(num_node-1):
					new_node_node = node_node.clone()

				#self.htree.dump()

			# clone other rack_nodes
			for i in range(num_rack-1):
				new_rack_node = rack_node.clone()

		#self.htree.dump()

		self.routers = []
		for router in xml_children(topo, u'router'):
			rt = empty_t()
			rt.connect_to_groups = []
			for connect_to_group in xml_children(router, u'connect_to_group'):
				rgname = str(xml_child_text(connect_to_group, u'rack_group_name'))
				switch = empty_t()
				switch.rg = self.find_hnode(tuple([int(rgname[5:])]))
				switch.index = int(xml_child_text(connect_to_group, u'switch_index'))
				rt.connect_to_groups.append(switch)

			rt.name = str(xml_child_text(router, u'name'))
			self.routers.append(rt)

		self.data_nodes = int(xml_child_text(topo, u'data_nodes'))
		self.job_tracker = str(xml_child_text(topo, u'job_tracker'))
		topology = xml_children(topo, u'topology')
		if len(topology) > 0 :
			self.topology = str(xml_text(topology[0]))
		else:
			self.topology = None

	def find_hnode(self, index_stack):
		if len(index_stack) > 5:
			print 'Wrong index stack' + index_stack
			return None

		node = self.htree
		for i in index_stack:
			children = node.children()
			node = children[i]
		return node

	def totcl(self, topo_tcl):
		f = open(topo_tcl, 'w')

		f.write('set int_bw %s\n' % (int_bw))
		f.write('set int_latency %s\n' % (int_latency))
		num_of_nodes = 0

		if self.topology == 'dcell':
			# special case, assume everything is symmetric
			#  take the first ng to get mt (machine type)
			#  number of nodes in a rack matters,
			#  number of racks does not matter.
			rg = self.htree.children()[0]
			racks = len(rg.children())
			r = rg.children()[0]
			ng = r.children()[0]
			nodes = len(ng.children())
			mt = ng.reserved
			f.write('set cpu_freq %f\n' % (freq_table[mt.cpu.type]))
			f.write('set cpu_cores %d\n' % (mt.cpu.cores * mt.cpu.num))
			f.write('set rbw %f\n' % (read_bw_table[mt.disk.type]))
			f.write('set wbw %f\n' % (write_bw_table[mt.disk.type]))
			f.write("\nset num_of_nodes %d\n" % (self.data_nodes))
			f.write('setup_2level_dcell %d\n' % (nodes))
			f.write('\n')
			f.write('set jt $%s\n' % (self.job_tracker))
			f.write('set racks %d\n' % (racks))
			f.write('set nodes %d\n' % (nodes))
			f.write('set data_nodes %d\n' % (self.data_nodes))
			f.write('set_mapnodes %d %d %d\n' % (racks, nodes, self.data_nodes))
			f.write('\n')
			f.close()
			return

		for rg in self.htree.children():
			self.racks = len(rg.children())
			for r in rg.children():
				f.write('set %s [$ns node]\n' % (r.name()))
				for ng in r.children():
					self.nodes = len(ng.children())
					mt = ng.reserved

					# cpu information for all nodes in a node group
					freq = freq_table[mt.cpu.type]
					cores = mt.cpu.cores * mt.cpu.num
					# disk read and write bandwidths
					rbw = read_bw_table[mt.disk.type]
					wbw = write_bw_table[mt.disk.type]

					f.write('for {set i 0} {$i < %d} {incr i} {\n' \
								% (len(ng.children())))
					f.write('\tnewnode "%s_$i" $%s\n' % (ng.name(), r.name()))
					num_of_nodes += len(ng.children())
					#f.write('\t$n30 set freq %f\n' % (freq))
					f.write('\t$n30 set tasklist [new MRPerf/TaskList %f %d]\n' % (freq, cores))
					f.write('\tfor {set j 0} {$j < %d} {incr j} {\n' \
								% (mt.disk.num))
					f.write('\t\t$n30 newdisk %f %f\n' % (rbw, wbw))
					f.write('\t}\n')
					f.write('}\n')
				f.write('\n')
		if True:
			# Guanying 2009.3.10: add a dedicated jobtracker
			# it does not count into num_of_nodes
			rg = self.htree.children()[0]
			r = rg.children()[0]
			ng = r.children()[0]
			mt = ng.reserved
			# cpu information for all nodes in a node group
			freq = freq_table[mt.cpu.type]
			cores = mt.cpu.cores * mt.cpu.num
			# disk read and write bandwidths
			rbw = read_bw_table[mt.disk.type]
			wbw = write_bw_table[mt.disk.type]

			'''
			jt = ng.name()+'_jobtracker'
			f.write('\nnewnode "%s" $%s\n' % (jt, r.name()))
			f.write('set jt $%s\n' % (jt))
			f.write('$jt set tasklist [new MRPerf/TaskList %f %d]\n' % (freq, cores))
			f.write('for {set j 0} {$j < %d} {incr j} {\n' \
						% (mt.disk.num))
			f.write('\t$jt newdisk %f %f\n' % (rbw, wbw))
			f.write('}\n')'''
		#f.write("\nset num_of_nodes %d\n" % (num_of_nodes))
		f.write("\nset num_of_nodes %d\n" % (self.data_nodes))

		for rt in self.routers:
			f.write('set %s [$ns node]\n' % (rt.name))
			f.write('$%s shape hexagon\n' % (rt.name))
			f.write('\n')
			for switch in rt.connect_to_groups:
				for r in switch.rg.children():
					f.write('$ns duplex-link $%s $%s %s %s DropTail\n' \
								% (r.name(), rt.name, ext_bw, ext_latency))

		f.write('\n')
		f.write('set jt $%s\n' % (self.job_tracker))
		f.write('set racks %d\n' % (self.racks))
		f.write('set nodes %d\n' % (self.nodes))
		f.write('set data_nodes %d\n' % (self.data_nodes))
		f.write('set_mapnodes %d %d %d\n' % (self.racks, self.nodes, self.data_nodes))
		f.write('\n')
		f.close()

	def totcl2(self, mapnodes_tcl):
		f = open(mapnodes_tcl, 'w')

		for rg_id in range(len(self.htree.children())):
			rg = self.htree.children()[rg_id]
			racks = len(rg.children())
			f.write('for {set i 0} {$i < %d} {incr i} {\n' % (racks))
			r = rg.children()[0]
			for ng_id in range(len(r.children())):
				ng = r.children()[ng_id]
				nodes = len(ng.children())
				f.write('\tfor {set j 0} {$j < %d} {incr j} {\n' % (nodes))
				n = ng.children()[0]

				'''
		set mn [format "%s%s%s%s" "\$n_rg0_" $i "_ng0_" $j]
		set tcp0 [new Agent/TCP/FullTcp]
		set dummy [new MRPerf/NodeApp $tcp0]
		eval "$dummy set hnode $mn"
		set app11 [$dummy new-connection $jt]
		$ns at 0.05 "$app11 snd {heartbeat}"
'''

				f.write('\t\tset mn [format "%%s%%s%%s%%s" "\\$n_rg%d_" $i "_ng%d_" $j]\n' % (rg_id, ng_id))
				f.write('\t\tset tcp0 [new Agent/TCP/FullTcp]\n')
				f.write('\t\tset dummy [new MRPerf/NodeApp $tcp0]\n')
				f.write('\t\teval "$dummy set hnode $mn"\n')
				f.write('\t\tset app11 [$dummy new-connection $jt]\n')
				f.write('\t\t$ns at 0.05 "$app11 send_heartbeat"\n')
				f.write('\t}\n')
			f.write('}\n')

		f.write('\n')
		f.close()

class conf_t:
	def __init__(self, gen_xml):
		root = xml.dom.minidom.parse(gen_xml)
		conf = xml_children(root, u'conf')[0]
		self.path = str(xml_child_text(conf, u'path'))

		files_node = xml_children(conf, u'number_files')[0]
		self.files = empty_t()
		self.files.min = int(xml_child_text(files_node, u'min_files'))
		self.files.max = int(xml_child_text(files_node, u'max_files'))

		size_node = xml_children(conf, u'file_size')[0]
		self.size = empty_t()
		self.size.unit_size = int(xml_child_text(size_node, u'unit_size')) #byte
		global chunk_size
		chunk_size = self.size.unit_size
		self.size.min_unit = int(xml_child_text(size_node, u'min_unit'))
		self.size.max_unit = int(xml_child_text(size_node, u'max_unit'))

		self.replicas = int(xml_child_text(conf, u'replication_level'))
		self.method = str(xml_child_text(conf, u'gen_method'))
		self.name_node = str(xml_child_text(conf, u'name_node'))

		#TODO: move into xml
		self.factor = 0.5

class job_t:
	def __init__(self, job_xml):
		root = xml.dom.minidom.parse(job_xml)
		job_node = xml_children(root, u'job')[0]
		self.tcl = 'set cycles_per_byte ' + \
				str(xml_child_text(job_node, u'cycles_per_byte')) + \
				'\n\t# in cycles per byte, 1G cycles per 1GB\n\n'
				
		filter_ratio_node = xml_children(job_node, u'filter_ratio')[0]
		distr_node = [node for node in filter_ratio_node.childNodes \
					 if node.nodeType == node.ELEMENT_NODE][0]

		s = str(distr_node.nodeName)
		self.tcl += 'set filter_ratio [new RandomVariable/%s]\n' % \
					(s.capitalize())
		if (distr_node.nodeName == u'constant'):
			self.tcl += '$filter_ratio set val_ %s \n' % (xml_text(distr_node))
		elif (distr_node.nodeName == u'uniform'):
			self.tcl += '$filter_ratio set min_ ' + \
					str(xml_child_text(distr_node, u'uniform_min')) + '\n'
			self.tcl += '$filter_ratio set max_ ' + \
					str(xml_child_text(distr_node, u'uniform_max')) + '\n'
		elif (distr_node.nodeName == u'pareto'):
			self.tcl += '$filter_ratio set avg_ %s\n' % \
					(xml_child_text(distr_node, u'pareto_scale'))
			self.tcl += '$filter_ratio set shape_ %s\n' % \
					(xml_child_text(distr_node, u'pareto_shape'))
		elif (distr_node.nodeName == u'exponential'):
			self.tcl += '$filter_ratio set avg_ %f\n' % \
					(1/float(xml_child_text(distr_node, u'exp_lambda')))
		elif (distr_node.nodeName == u'normal'):
			self.tcl += '$filter_ratio set avg_ %s\n' % \
					(xml_child_text(distr_node, u'normal_average'))
			self.tcl += '$filter_ratio set std_ %s\n' % \
					(xml_child_text(distr_node, u'normal_variance'))
		else:
			print 'warning: unknown distribution method'
		self.tcl += '\n'

		self.tcl += 'set avg_record_size %s\n\t# in byte\n' % \
				(xml_child_text(job_node, u'average_record_size'))
		self.tcl += 'set jt $%s\n' % (xml_child_text(job_node, u'job_tracker'))
		self.input = str(xml_child_text(job_node, u'input_dir'))
		self.output = str(xml_child_text(job_node, u'output_dir'))

		self.tcl += '\n'

	def _const(self):
		return self.constant

	def _uniform(self):
		return random.uniform(self.uniform.min, self.uniform.max)

	def _pareto(self):
		return random.paretovariate(self.pareto.alpha)

	def _gauss(self):
		return random.gauss(self.gauss.mu, self.gauss.sigma)

	def _expo(self):
		return random.expovariate(self.expo.lambd)

global_i = 0

def get_new_filename():
	global global_i
	filename = 'file_'+str(global_i).zfill(8)
	global_i += 1
	return filename

class disk_t:
	def __init__(self, rg, rack, ng, node, disk):
		self.rg = rg
		self.rack = rack
		self.ng = ng
		self.node = node
		self.disk = disk

	def name(self):
		return 'n_rg%d_%d_ng%d_%d_disk%d' % (rg, rack, ng, node, disk)

global_last_chunk_on_disk = None
global_linear_chunk_index = 0

class distribute_linear:
	def __init__(self, topo):
		self.topo = topo

	def distribute_chunk(self, replicas=1):
		'''search the tree to find a node with used < capacity().'''

		'''wanggy 2008.7.9:
Note that the function always generate only 1 replica for each chunk.
That is because of the nature of linear distribution. It's not easy to
define a linear way to generate multiple copies for each chunk, on different
disks. Maybe a possible option is to divide the storage space into several
parts, and put 1 replica in each part.
parameter replicas here can be more than 1, but the function will not
produce more than 1 replicas for that.'''

		node = self.topo.htree
		if node.used >= node.capacity():
			print 'error: HDFS full'
			return ['error: HDFS full']
		while node.children() <> []:
			for child in node.children():
				if child.used < child.capacity():
					break
			node = child
		node.add_chunk()
		return [node]

class distribute_RR:
	def __init__(self, topo):
		self.order = []
		stack = []
		self.last_disk = -1
		self.topo = topo

		stack.append(topo.htree)
		while stack <> []:
			node = stack.pop()
			if node.children() <> []:
				temp = node.children()[:]
				temp.reverse()
				for child in temp:
					stack.append(child)
			else:
				# reach a disk
				# assert len(node.index_stack) == 5
				self.order.append(node)

	def distribute_chunk(self, replicas=1):
		'''This is a round robin algorithm on disk level, not on node level.'''
		#cycle = len(self.order)
		# Guanying 2009.4.7: workaround for data_nodes
		cycle = self.topo.data_nodes
		i = (self.last_disk + 1) % cycle
		#print i, self.last_disk

		disklist = []
		while len(disklist) < replicas:
			disk = self.order[i]
			if disk.used < disk.capacity():
				disk.add_chunk()
				if disk in disklist:
					print 'warning: round robin for multiple replicas has ' \
							'iterated around all disks.'
					break
				disklist.append(disk)
				i = (i + 1) % cycle
			else :
				del self.order[i]
				if self.order == []:
					print "error: HDFS full"
					return ["error: HDFS full"]
				i = i % cycle
		self.last_disk = (i-1) % cycle
		#print i, self.last_disk
		return disklist

class distribute_random:
	def __init__(self, topo):
		self.topo = topo

	def distribute_chunk(self, replicas=1):
		return self.distribute_chunk_random(replicas)

	def distribute_chunk_random(self, replicas=1):
		'''random at disk level. replicas guaranteed to be on different disks,
but maybe on disks on the same node. Then when viewed at node level, two or
more replicas can be on the same node. I don't consider that as an error.
If it is preferred to have replicas on different nodes, I can change that
later'''
		# TODO: comments above

		disklist = []
		while len(disklist) < replicas:
			disk = self.distribute_one_chunk()
			if disk == None:
				break
			if not disk in disklist:
				disklist.append(disk)

		for disk in disklist:
			disk.add_chunk()

		return disklist

	def distribute_one_chunk(self):
		while True:
			node = self.topo.htree
			if node.capacity() <= node.used:
				print 'error: HDFS full\n'
				return None
			index = random.randrange(node.capacity()-node.used)

			while node.children() <> []:
				temp = node
				for child in node.children():
					if child.capacity() - child.used > index:
						node = child
						break
					else:
						index -= (child.capacity() - child.used)
				if temp.index_stack == node.index_stack:
					print 'error\n'
					print node.index_stack
					print node.capacity(), index, node.global_end()
					print node.global_end(), node.used, index
					print child.global_end(), child.used, index
					sys.exit()

			# Guanying 2009.4.7: workaround for data_nodes
			r = node.index_stack[1]
			n = node.index_stack[3]
			#print r, n
			if r*self.topo.nodes + n >= self.topo.data_nodes:
				continue

			if node.used < node.capacity():
				break

		return node

	def is_invalid_datanode(self, node):
		'''Guanying 2009.4.7: workaround for data_nodes'''
		r = node.index_stack[1]
		n = node.index_stack[3]
		#print r, n
		return (r*self.topo.nodes + n >= self.topo.data_nodes)

	def choose_hnode(self, exclude_nodes=None, level=4, subtree=None):
		if exclude_nodes == None:
			exclude_nodes = []
		if subtree == None:
			subtree = self.topo.htree
		while True:
			node = subtree
			if node.capacity() <= node.used:
				print 'error: HDFS full at %s\n' % (node.index_stack)
				return None
			index = random.randrange(node.capacity()-node.used)

			while len(node.index_stack) < level:
				temp = node
				for child in node.children():
					if child.capacity() - child.used > index:
						node = child
						break
					else:
						index -= (child.capacity() - child.used)
				if temp.index_stack == node.index_stack:
					print 'error\n'
					print node.index_stack
					print node.capacity(), index, node.global_end()
					print node.global_end(), node.used, index
					print child.global_end(), child.used, index
					sys.exit()

			if node.used < node.capacity() and not node in exclude_nodes:
				break

		exclude_nodes.append(node)
		return (node, exclude_nodes)

class distribute_Hadoop(distribute_random):
	def __init__(self, topo):
		self.topo = topo
		racks = 0
		for rg in topo.htree.children():
			racks += len(rg.children())

		if racks < 2:
			self.one_rack = True
		else:
			self.one_rack = False

	def distribute_chunk(self, replicas=1):
		if self.one_rack:
			return self.distribute_chunk_random(replicas)
		else:
			local_node, ex = self.choose_node()
			while self.is_invalid_datanode(local_node):
				local_node, ex = self.choose_node()
				#print local_node.index_stack
			return self.distribute_chunk_Hadoop(local_node, replicas)

	def distribute_chunk_Hadoop(self, local_node, replicas=1):
		'''Always on more than one racks. First chunk on local node, second
in same rack, third on another rack. Problem is: which node is local?
Answer 1: Randomly choose a local node.'''
		ex = [local_node]
		rack = local_node.parent.parent
		another_node, ex1 = self.choose_node(rack, ex)
		while self.is_invalid_datanode(another_node):
			another_node, ex1 = self.choose_node(rack, ex)
			#print another_node.index_stack
		ex = ex1
		another_rack, ex = self.choose_rack([rack])
		remote_node, ex1 = self.choose_node(another_rack, [])
		while self.is_invalid_datanode(remote_node):
			remote_node, ex1 = self.choose_node(another_rack, [])
			#print remote_node.index_stack

		disklist = [node.choose_disk() for node in [local_node, another_node, remote_node]]
		for disk in disklist:
			if disk <> None:
				disk.add_chunk()

		#print [disk.index_stack for disk in disklist]
		return disklist

	def choose_node(self, subtree=None, exclude_nodes=None):
		return self.choose_hnode(exclude_nodes, 4, subtree)

	def choose_rack(self, exclude_racks=None):
		return self.choose_hnode(exclude_racks, 2, self.topo.htree)


def gen(topo, conf):
	method_dict = {'linear': distribute_linear,
				'RR': distribute_RR,
				'random': distribute_random,
				'Hadoop': distribute_Hadoop}
	method = method_dict[conf.method](topo)

	xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<root xsi:noNamespaceSchemaLocation="metadata.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<chunk_size>16</chunk_size>
	<name_node>1</name_node>
	<dir name="data">
		<file name="file">
			<chunk id="0">
				<rep>n1</rep>
			</chunk>
		</file>
	</dir>
</root>
"""

	meta = xml.dom.minidom.parseString(xml_template)
	root = xml_children(meta, u'root')[0]

	chunk_size_node = xml_children(root, u'chunk_size')[0].childNodes[0]
	chunk_size_node.nodeValue = unicode(str(chunk_size), 'utf-8')

	name_node_text_node = xml_children(root, u'name_node')[0].childNodes[0]
	name_node_text_node.nodeValue = unicode(conf.name_node, 'utf-8')

	dir = xml_children(root, u'dir')[0]
	dir.setAttribute(u'name', unicode(conf.path, 'utf-8'))

	file_node = xml_children(dir, u'file')[0]
	chunk_node = xml_children(file_node, u'chunk')[0]
	replica_node = xml_children(chunk_node, u'rep')[0]

	if (conf.files.min+conf.files.max)/2 \
				* (conf.size.min_unit+conf.size.max_unit)/2 \
				* conf.size.unit_size * conf.replicas \
				>= conf.factor * topo.htree.capacity():
		print "warning: too much data to be stored in DFS\n"

	files = int(random.uniform(conf.files.min, conf.files.max))
	for i in range(files):
		new_file = file_node.cloneNode(False) # the param is 'deep',
							#means to clone all child nodes as well.
		name = get_new_filename()
		new_file.setAttribute(u'name', unicode(name, 'utf-8'))
		dir.appendChild(new_file)

		p = zipf(1.5, conf.size.max_unit).next()
		if p < conf.size.min_unit:
			p = conf.size.min_unit
		chunks = int(p \
					 * conf.size.unit_size / chunk_size)
#		if i % 10 == 0:
#			print i

		for j in range(chunks):
			new_chunk = chunk_node.cloneNode(False)
			new_chunk.setAttribute(u'id', unicode(str(j), 'utf-8'))
			new_file.appendChild(new_chunk)

			for node in method.distribute_chunk(conf.replicas):
				new_replica = replica_node.cloneNode(True)
				text_node = new_replica.childNodes[0]
				text_node.nodeValue = unicode(node.name(), 'utf-8')
				new_chunk.appendChild(new_replica)

	#TODO: make xml prettier. see results from the following two commented lines
	#print file_node.childNodes
	#print new_file.childNodes

	dir.removeChild(file_node)
	file_node.unlink()

	#verify(dir)

	return meta

def verify(dir_node):
	for file_node in xml_children(dir_node, u'file'):
		filename = str(file_node.getAttribute(u'name'))

		for chunk_node in xml_children(file_node, u'chunk'):
			chunk_id = int(chunk_node.getAttribute(u'id'))

			if len(xml_children(chunk_node, u'rep')) != 3:
				print chunk_node
			list = []
			for replica_node in xml_children(chunk_node, u'rep'):
				replica = str(replica_node.childNodes[0].nodeValue)
				if replica in list:
					print 'error!!!'
				list.append(replica)


def main():
	global options
	usage = "usage: %prog options"
	parser = OptionParser(usage)
	parser.add_option("-v", "--verbose", default=False,
					action="store_true", dest="verbose")
	parser.add_option("-t", "--topology", dest="topo_xml",
					help="topology configuration xml")
	parser.add_option("-g", "--gen", dest="gen_xml", 
					help="metadata generation configuration xml")
	parser.add_option("-m", "--metadata", dest="meta_xml", 
					help="metadata configuration xml")
	(options, args) = parser.parse_args()

	if None in (options.topo_xml, options.meta_xml, options.gen_xml):
		print 'xmls not defined'
		parser.print_help()
		sys.exit()

	random.seed()

	topo = topology_t(options.topo_xml)
	conf = conf_t(options.gen_xml)
	meta = gen(topo, conf)

	f = open(options.meta_xml, 'w')
	f.write(meta.toxml())
	f.close()

#	f = open(options.meta_xml)
#	print f.read()
#	f.close()

if __name__ == '__main__':
	main()

