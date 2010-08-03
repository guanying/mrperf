#!/usr/bin/python

import random
import math
from gen import *
from optparse import OptionParser

def gen_trace(rate, maxtime, app_list, topo, conf, iseed=None):
	if iseed == None:
		random.seed()
		iseed = random.random()

	random.seed(iseed)
	arrival = 0
	arrival_list = []
	while arrival < maxtime:
		interarrival = random.expovariate(rate)
		arrival += interarrival
		arrival_list.append(arrival)

	class empty:
		pass

	class empty_with_ID:
		id = 0
		def __init__(self):
			empty_with_ID.id += 1
			self.id = empty_with_ID.id

	for arrival in arrival_list:
		job = empty_with_ID()
		job.arrival = arrival
		job.app_id = random.randrange(len(app_list))
		app = app_list[job.app_id]
		print "new_job", job.arrival, app[0],
		job.params = []
		for arange in app[1]:
			param = math.exp(random.uniform(math.log(arange[0]), math.log(arange[1])))
			job.params.append(param)
			print param,
		print

		meta = gen(topo, conf)

		f = open("job_%04d_data.xml" % (job.id), 'w')
		f.write(meta.toxml())
		f.close()

def main():
	global options
	usage = "usage: %prog options"
	parser = OptionParser(usage)
	parser.add_option("-v", "--verbose", default=False,
					action="store_true", dest="verbose")
	parser.add_option("-t", "--topology", dest="topo_xml",
					default='topology1.xml',
					help="topology configuration xml")
	parser.add_option("-g", "--gen", dest="gen_xml", 
					default='metadata_gen1.xml',
					help="metadata generation configuration xml")
	parser.add_option('-n', '--type', dest='type', default='-1')
	(options, args) = parser.parse_args()

	if None in (options.topo_xml, options.gen_xml):
		print 'xmls not defined'
		parser.print_help()
		sys.exit()

	random.seed()

	topo = topology_t(options.topo_xml)
	conf = conf_t(options.gen_xml)

	from hadoop_conf import rate, maxtime, app_list
	type = int(options.type)
	if type >= 0:
		app_list = app_list[type:type+1]

	gen_trace(rate, maxtime, app_list, topo, conf)


if __name__ == '__main__':
	main()

