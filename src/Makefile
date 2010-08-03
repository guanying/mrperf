
all: hsim

hsim:
	rm -f stats_*
	ns hsim.tcl

meta: topology1.xml metadata_gen1.xml gen.py
	make gen

gen:
	## create random.xml as Data Layout (metadata)
	#./gen.py -t topology1.xml -g metadata_gen1.xml -m random.xml
	## updated - generate a trace of jobs, each with its own Data Layout.
	rm -f job_*.xml
	python multi-job-gen.py -t topology1.xml -g metadata_gen1.xml > job.trace.tcl

topo:
	## create hadoop.topo.tcl (topology)
	./conv.py -t topology1.xml -m random.xml -j job1.xml

sim: hsim


hadoop.topo.tcl: topology1.xml conv.py
	./conv.py -t topology1.xml -m random.xml -j job1.xml

distclean:
	rm -rf out.* *.pyc stat* random.xml job_*.xml stats_* job.trace.tcl hadoop.topo.tcl

small:
	cp ../conf/t-small.xml topology1.xml
	cp ../conf/mg-small.xml metadata_gen1.xml
	cp ../conf/job-small.tcl hadoop.job.tcl

test: small topo gen all

trace: gen

