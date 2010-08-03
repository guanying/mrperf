proc init_Terasort {job} {
	$job set jvm_start_cost [expr 4.0*1000*1000*1000]
	$job set wrap_up_cost [expr 4.8*1000*1000*1000]
	$job set io_factor 10
	$job set in_mem_space [expr 100.0*1024*1024 * 0.66]  ;# 100MB*0.66
	$job set in_mem_segments 100

	#mappers
	$job set cycles_per_byte 40.0
		# in cycles per byte, 1G cycles per 1GB
	$job set sort_cycles_per_byte 44.0
	$job set merge_cycles 1.0*1000*1000*1000
	#$job set dist_cache_size 1.0*1024*1024

	#$job set start_sort_when_buffer 1
	$job set buffer_size [expr 50*1024*1024]
	$job set filter_ratio [new RandomVariable/Uniform]
	[$job set filter_ratio] set min_ 1
	[$job set filter_ratio] set max_ 1

	#reducers
	$job set no_copiers $::num_of_nodes
	#$job set no_copiers 5
	$job set reduce_sort_cycles 6.0*1000*1000*1000
	$job set reduce_cycles_per_byte 90.0


	# on the above two parameters, if num_of_reducers is larger than max_reducers
	# times number of nodes, then some reducers are started first, and other
	# reducers are not started until some reducers finishes.
	$job set reduce_filter_ratio [new RandomVariable/Uniform]
	[$job set reduce_filter_ratio] set min_ 1.0
	[$job set reduce_filter_ratio] set max_ 1.0
}

proc init_Search {job cycles_per_byte} {
	init_Terasort $job
	$job set cycles_per_byte $cycles_per_byte
	[$job set filter_ratio] set min_ 0.0
	[$job set filter_ratio] set max_ 0.0001
}

proc init_Index {job filter_ratio} {
	init_Terasort $job
	[$job set filter_ratio] set min_ $filter_ratio
	[$job set filter_ratio] set max_ $filter_ratio
}

proc init_Compute {job cycles_per_byte} {
	init_Terasort $job
	$job set cycles_per_byte $cycles_per_byte
	[$job set filter_ratio] set min_ 1.0
	[$job set filter_ratio] set max_ 10.0
}

