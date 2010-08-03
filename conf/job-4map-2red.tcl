
set avg_record_size 100
	# in byte.
	# not in use --Guanying 2008.1.11
#set jt $n_rg0_0_ng0_1

source parameters.tcl

# mappers
set max_mappers 4
	# per node
set start_sort_when_buffer 1
set buffer_size [expr 50*1024*1024]
set filter_ratio [new RandomVariable/Uniform]
$filter_ratio set min_ 1
$filter_ratio set max_ 1

# reducers
set max_reducers 2
	# concurrent reducers per node.
set num_of_reducers [expr $max_reducers*$num_of_nodes]
	# total reducers to start
# on the above two parameters, if num_of_reducers is larger than max_reducers
# times number of nodes, then some reducers are started first, and other
# reducers are not started until some reducers finishes.
set reduce_filter_ratio [new RandomVariable/Uniform]
$reduce_filter_ratio set min_ 1.0
$reduce_filter_ratio set max_ 1.0

