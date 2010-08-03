
MRPerf/MapTask instproc schedule {hnode} {
	$self def-schedule $hnode
	#$self remote-schedule $hnode
}

MRPerf/MapTask instproc add-counter {name} {
	#global ns
	[$self set counters] set $name [expr [$::ns now] * 1000]
}

MRPerf/MapTask instproc add1-counter {counter name} {
	#global ns
	$counter set $name [expr [$::ns now] * 1000]
}

MRPerf/MapTask instproc write-counters {} {
	set c [$self set counters]

	set counters "File Systems.Local bytes read:[$self set output_size],File Systems.Local bytes written:[expr 2*[$self set output_size]],File Systems.HDFS bytes read:[$self set input_size],Performance Counters.INIT:[$c set init],Performance Counters.MAP_START:[$c set map_start],Performance Counters.MAP_FINISH:[$c set map_finish]"

	for {set i 0} {$i < [$self set spill]} {incr i} {
		set p [$c set processes($i)]
		set counters [format "%s%s%s%s" "$counters,Performance Counters.$i" "_SORT_START:[$p set sort_start],Performance Counters.$i" "_SORT_FINISH:[$p set sort_finish],Performance Counters.$i" "_SPILL_FINISH:[$p set spill_finish]" ]
	}

	if {[$self set has_merge] == 1} {
		set counters "$counters,Performance Counters.MERGE_START:[$c set merge_start],Performance Counters.MERGE_FINISH:[$c set merge_finish]"
	}

	$self instvar job
	puts [$job set stats] "Task TASKID=\"$self\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"[$self set finish_time]\" COUNTERS=\"$counters\""
}

MRPerf/NodeApp instproc jvm-start {task} {
	#jvm_start_cost
	#puts "jvm-start $task"
	set n [$task worker]

	$self inserts "$task add-counter init"

	#puts "****** jvm $jvm_start_cost ****************"
	$self insertd [expr 1.0*[[$self set job] set jvm_start_cost]] $n
}


MRPerf/NodeApp instproc spill {task size} {
	#sort_cycles_per_byte 

	#puts "spill $task $size"

	# spill to the same disk? or different disks?
	# wanggy 2008.7.24: assume a single-disk-per-node model
	set node [$task worker]
	set disk [$node choosedisk]

	set p [new MRPerf/Empty]
	[$task set counters] set processes([$task set spill]) $p
	$task set spill [expr [$task set spill] + 1]

	$task set output_size [expr [$task set output_size] + $size]

	$self inserts "$task add1-counter $p sort_start"
	$self insertd [expr 1.0*$size * [[$self set job] set sort_cycles_per_byte]] $node
	$self inserts "$task add1-counter $p sort_finish"
	$self iowrite $size $disk
	$self inserts "$task add1-counter $p spill_finish"
}

MRPerf/NodeApp instproc readsize {task offset size} {
	#puts "readsize $task $offset $size"
	set disk [$task disk]
	$self ioread $size $disk
}

MRPerf/NodeApp instproc merge {task} {
	#puts "merge $task"

	set node [$task worker]
	set disk [$node choosedisk]

	set length [$task set output_size]

	$task set has_merge 1

	set c [$task set counters]
	$self inserts "$task add1-counter $c merge_start"
	$self ioread $length $disk
	$self iowrite $length $disk
	$self inserts "$task add1-counter $c merge_finish"
}

MRPerf/NodeApp instproc compute {task offset size} {
	#puts "compute $task $offset $size"
	#cycles_per_byte
	set n [$task worker]
	$self insertd [expr 1.0* $size * [[$self set job] set cycles_per_byte]] $n
}

MRPerf/NodeApp instproc wrap-up {task} {
	#wrap_up_cost
	set n [$task worker]
	$self insertd [[$self set job] set wrap_up_cost] $n
}

MRPerf/MapTask instproc mark_ghost {} {
	if {[$self info vars ghost] != ""} {
		return
	}
	$self set ghost 1

	#no_maptasks_to_start finished_maptasks
	$self cpp_fail
	# does a MapTask has reference to Job?
	[$self set job] set no_maptasks_to_start [expr [[$self set job] set no_maptasks_to_start] + 1]

	[[$self set job] set finished_maptasks] erase $self
}

MRPerf/MapTask instproc fail {} {
	#global ns sched
	#no_maptasks_to_start
	puts "[$::ns now]: map $self fail"
	puts [[$self set job] set stats] "MapAttempt TASKID=\"$self\" TASK_STATUS=\"KILLED\" FINISH_TIME=\"[expr [$::ns now]*1000]\" HOSTNAME=\"[[$self worker] hostname]\""

	if {[$self info vars alive_timer] != ""} {
		#$::ns cancel [$self set alive_timer]
		[$self set alive_timer] cancel
		delete [$self set alive_timer]
		$self unset alive_timer
	}
	#$::ns cancel [[$self set expire_timer] set eid]
	[$self set expire_timer] cancel
	delete [$self set expire_timer]
	$self unset expire_timer


	$self cpp_fail
	#incr no_maptasks_to_start
	[$self set job] set no_maptasks_to_start [expr [[$self set job] set no_maptasks_to_start] + 1]
	set nrm [[$self worker] set running_maptasks]
	[$self worker] set running_maptasks [expr $nrm-1]
}

MRPerf/NodeApp instproc send_alive {task} {
	#global ns timers
	$self snd "alive $task"
	#$task set alive_timer [$::ns after $::timers(alive) "$self send_alive $task"]
}

