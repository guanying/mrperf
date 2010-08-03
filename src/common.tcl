

# wanggy 2008.7.3
# ----------------------

# This is an adapted tcl file from tcpapp.tcl in ns-2. I use this file because
# Application level data can be transferred on top of tcp. A simple protocol
# runs here and needs the ability.
# 
# Here a class MRPerf/NodeApp is defined. in C++ code (webcache/tcpapp.[cc|h]),
# MRPerf/Node and MRPerf/Scheduler are defined and mapped to tcl. MRPerf/Chunk
# and MRPerf/DataLayout are not mapped to tcl. a chunk is referred to by a
# string in tcl, and looked up in C++.
# 
# A MRPerf/Node (hnode) is a wrapper object for ns-2 node, and can save some
# useful information for the node. A MRPerf/NodeApp is one end of a connection
# between two nodes. A connection is created by new-connection(), and two
# MRPerf/NodeApp objects are created as two ends. Most operations are
# MRPerf/NodeApp oriented, in the sense that as long as App is referred to,
# everything can be found.
# 
# One scheduler (singleton) is used in the code. When it is constructed,
# metadata.xml is parsed by DataLayout singleton. Tcl cannot see DataLayout,
# and performs all operations on DataLayout through Scheduler.


Class MRPerf/NodeApp -superclass {Application/TcpApp}
Class MRPerf/LoopApp -superclass {MRPerf/NodeApp}
Class MRPerf/Empty

# wanggy 2008.11.26
# ------------------------------
# Chain class is an abstraction of a series of tasks to be run serially.  The
# head of a Chain is the currently running task.  Other tasks in a Chain will
# be run after the first (head) task finishes.  That's how a thread works.
# However, Chain/TaskList is more like an event model.  An event happens,
# optionally lasts for some time, and then next event is scheduled.
#
# Two kinds of tasks are available: simple task and device task.  A simple task
# does not work on devices (cpu/disk), which is usually sending a network
# message.  A simple task is inserted in to a Chain, and when it becomes the
# head of the Chain, it is executed, and Chain will immediately move on to the
# next task.  A device task is an operation on a device (cpu/disk), which will
# take some time.  It is also inserted into a Chain, and when it becomes head
# of the Chain, it is put into a TaskList.  When TaskList determines it
# finishes, the task is done, and the Chain moves on to the next task.
#
# A TaskList shares available resources among all concurrently running tasks.
# Every time a task finishes, or a task is inserted, TaskList do a recalculate,
# to find out which task will finish next, and schedule a wake-up at that time.
# The same process of recalculation goes on again then.

# wanggy 2009.1.5
# the above comment (2008.11.26) was on an implementation in tcl. Now this
# is moved to C++.

Class MRPerf/Chain -superclass {MRPerf/Thread/Chain}
Class MRPerf/TaskList -superclass {MRPerf/Thread/TaskList}
Class MRPerf/Task -superclass {MRPerf/Thread/Task}

set epsilon 10000


MRPerf/Job instproc run {} {
	#global ns sched
	$self set start_time [$::ns now]
	puts "job $self started at [$::ns now]"
	$::sched start-job $self
}

MRPerf/Job instproc finish {} {
	#global job_queue ns sched
	#global trace_length
	$self set finish_time [$::ns now]
	puts "job $self finished at [$::ns now]"
	$::sched finish-job $self
	close [$self set stats]

	# 2009.10.15 modify to share
	#$::job_queue next-task
	#if {[$::job_queue size] == 0 && [$::ns now] > $::trace_length} {
	#	finish
	#}
	if {[$::sched running-jobs] == 0 && [$::ns now] > $::trace_length} {
		finish
	}
	# 2009.10.15 done
}

MRPerf/NodeApp instproc out {text} {
	puts "[$self now]: $text"
}

MRPerf/NodeApp instproc out1 {text} {
	$self inserts "$self out {$text}"
}

#source io.tcl

proc finish {} {
	global ns total_time
	#global namfile nf f
	#$ns flush-trace
	#close $nf
	#close $f

	if {$total_time == "0"} {
		puts stderr "not finished till [$ns now]"
	} else {
		puts stderr "Total runtime: $total_time"
	}

	#$ns output-counters
	#puts "new-connection:        $::new_connection_count * 22.7ms = [expr $::new_connection_count * 0.0227] seconds"
	#puts "new-connection(gprof): $::new_connection_count * 37ms = [expr $::new_connection_count * 0.037] seconds"
	#set event_count [$ns at -0.1 "puts \"never shown\""]
	#puts $event_count

	#puts "running nam..."
	#exec nam $namfile &
	exit 0
}


MRPerf/NodeApp instproc now {} {
	#global ns
	return [string range [expr ([$::ns now] + [$self set time]) * 1000] 0 10]
}

MRPerf/NodeApp instproc at {delay args} {
	#global ns
	#$self instvar time

	#set t [$self set time]

	$self set time [expr [$self set time] + $delay]
	set later [expr [$::ns now] + [$self set time]]

	#puts "$self: $t + $delay = [$self set time] ($later)"

	$::ns at $later "$args"
}

MRPerf/NodeApp instproc dummy {} {
}

MRPerf/ReduceTask instproc getget {prefix name} {
	#global ns
	puts "[$::ns now]: $prefix $self [$self set $name]"
}

MRPerf/ReduceTask instproc addadd {name} {
	$self set $name [expr [$self set $name] + 1]
}

MRPerf/NodeApp instproc getget {prefix var name} {
	puts "[$self now]: $prefix $var [$var set $name]"
}

#source map.tcl
#source reduce.tcl

MRPerf/NodeApp instproc dump {} {
	set node [$self set hnode]
	set name [$node nodename]
	set rem  [[[$self dst] set hnode] nodename]
	return "$rem ([$self dst]) -> $name ($self)"
}

MRPerf/LoopApp instproc connect {rapp} {
	$self instvar dst
	$self set dst $rapp
	$rapp set dst $self
}

MRPerf/LoopApp instproc dst {} {
	return [$self set dst]
}

MRPerf/LoopApp instproc init {} {
}

MRPerf/LoopApp instproc send {length msg} {
	eval $msg
}

MRPerf/NodeApp instproc snd {msg} {
	$self send 100 "[$self dst] recv {$msg}"
}

MRPerf/NodeApp instproc shuffle_send_alive {} {
	#global ns timers
	$self snd "shuffle heartbeat"
	#puts "[$ns now]: $self sending heartbeat"


	#$self set shuffle_heartbeat_event [$::ns after $::timers(alive) "$self shuffle_send_alive"]
	#$self set shuffle_alive [new MRPerf/Util/AliveTimer $::timers(alive) "$self snd \"shuffle heartbeat"]


	#puts "[$ns now]: $self heartbeat"
}

MRPerf/NodeApp instproc send_heartbeat {} {
	#global ns heart_beat_delay
	$self snd "heartbeat"
	#puts "[$ns now]: $self sending heartbeat"


	#$::ns after $::heart_beat_delay "$self send_heartbeat"
	#$self set heartbeat_timer [new MRPerf/Util/AliveTimer $::heart_beat_delay "$self snd \"heartbeat\""]


	#puts "[$ns now]: $self heartbeat"
}

MRPerf/NodeApp instproc new-connection2 rhnode {
	set app [$self new-connection $rhnode]
	$app set job [$self set job]
	[$app dst] set job [$self set job]
	return $app
}

#set new_connection_count 0

MRPerf/NodeApp instproc new-connection rhnode {
	#global ns

	if { [$self set hnode] == $rhnode } {
		set loop0 [new MRPerf/LoopApp]
		set loop1 [new MRPerf/LoopApp]
		$loop0 set hnode $rhnode
		$loop1 set hnode $rhnode
		$loop0 set time 0
		$loop1 set time 0
		$loop0 set chain [new MRPerf/Chain]
		$loop1 set chain [new MRPerf/Chain]
		$loop0 connect $loop1

		$loop0 set parent $self
		$loop0 set job ""
		$loop1 set job ""
		return $loop0
	}

	#incr ::new_connection_count

	set tcp2 [new Agent/TCP/FullTcp]
	set tcp3 [new Agent/TCP/FullTcp]
	set local [[$self set hnode] set nsnode]
	set remote [$rhnode set nsnode]
	$::ns attach-agent $local $tcp2
	$::ns attach-agent $remote $tcp3
	$::ns connect $tcp2 $tcp3
	$tcp3 listen

	set app2 [new MRPerf/NodeApp $tcp2]
	set app3 [new MRPerf/NodeApp $tcp3]
	$app2 set hnode [$self set hnode]
	$app3 set hnode $rhnode
	$app2 set time 0
	$app3 set time 0
	$app2 set chain [new MRPerf/Chain]
	$app3 set chain [new MRPerf/Chain]
	$app2 set tcp $tcp2
	$app3 set tcp $tcp3
	$app2 set job ""
	$app3 set job ""

	# parent is the connection that creates this new one.
	# when this connection finishes, its parent is the way to go back.
	$app2 set parent $self
	$app2 connect $app3

	return $app2
}

if {0} {
proc connclose {app} {
	set remote [$app dst]
	delete [$app set chain]
	delete [$remote set chain]
	if { [$app set hnode] != [$remote set hnode] } {
		#delete [$app set tcp]
		#delete [$remote set tcp]
	}
	delete $app
	delete $remote
}

MRPerf/NodeApp instproc des1troy {} {
	#puts "destroy $self [$self dst]"
	set task [new MRPerf/Thread/Task]
	$task conf 2 "ns after 0.1 \"connclose $self\"" 0
	[$self set chain] insert $task
}
}

MRPerf/NodeApp instproc nsnode {} {
	return [[$self set hnode] set nsnode]
}

MRPerf/NodeApp instproc run_maptask { node } {
	set taskstr [new MRPerf/MapTask]
	set result [$::sched launch-task $node $taskstr]
	if {$result == "" } {
		delete $taskstr
		#puts "[$self now]: cannot find a maptask for $node"
		return 0
	}

	$taskstr set counters [new MRPerf/Empty]
	$taskstr set spill 0
	$taskstr set has_merge 0
	$taskstr set output_size 0
	$taskstr set job [$taskstr job]
	set job [$taskstr job]
	#puts "$job [$job status]"

	set app2 [$self new-connection $node]
	$app2 set job $job
	$app2 snd "map task $taskstr"
	puts [$job set stats] "Task TASKID=\"$taskstr\" TASK_TYPE=\"MAP\" START_TIME=\"[$self now]\" SPLITS=\"[[[$taskstr disk] host] hostname]\""
	$taskstr set expire_timer [new MRPerf/Util/Timer $::timers(task_dead) "$taskstr fail"]

	puts "[$self now]: $node run maptask $taskstr, written to [$job set stats_file]"
	return 1
}

if {0} {
MRPerf/NodeApp instproc start_maptask { node } {
	#global timers max_mappers sched

	if {[$node set running_maptasks] >= [$::sched max_mappers]} {
		#puts "$node already has [$node set running_maptasks] map tasks running!"
		return 0
	}

	set job [$::sched pick-job]
	if {$job == ""} {
		return 0
	}
	if {[$job set no_maptasks_to_start] <= 0} {
		return 0
	}
	set taskstr [new MRPerf/MapTask]
	set result [$::sched schedule $node $taskstr $job]
	if {$result != "" } {
		$taskstr set counters [new MRPerf/Empty]
		$taskstr set spill 0
		$taskstr set has_merge 0
		$taskstr set output_size 0
		$taskstr set job $job

		set app2 [$self new-connection $node]
		$app2 set job $job
		$app2 snd "map task $taskstr"
		puts [$job set stats] "Task TASKID=\"$taskstr\" TASK_TYPE=\"MAP\" START_TIME=\"[$self now]\" SPLITS=\"[[[$taskstr disk] host] hostname]\""
		#$node set uncounted [expr [$node set uncounted] + 1]
		$node set running_maptasks [expr [$node set running_maptasks]+1]
		$taskstr set expire_timer [new MRPerf/Util/Timer $::timers(task_dead) "$taskstr fail"]

		$job set no_maptasks_to_start [expr [$job set no_maptasks_to_start] - 1]
		#upvar #0 no_maptasks_to_start m
		#set m [expr $m-1]
		#puts "$m map tasks to start"
		return 1
	}
	delete $taskstr
	return 0

}
}

MRPerf/NodeApp instproc start_reducetask { node } {
	#global timers sched max_reducers
	#no_copiers num_of_nodes

	set job [$::sched pick-job-reduce]
	if {$job == ""} {
		return 0
	}
	if {[$job set no_reducetasks_to_start] <= 0} {
		return 0
	}
	set no_copiers [$job set no_copiers]
	if {[$node set running_reducetasks] >= $::max_reducers} {
		#puts "$node already has [$node set running_reducetasks] reduce tasks running!"
		return 0
	}


	set taskstr [new MRPerf/ReduceTask]
	set result [$::sched schedule-reduce $node $taskstr $job]
	if {$result != "" } {
		$taskstr set counters [new MRPerf/Empty]
		$taskstr set input_size 0
		$taskstr set output_size 0
		$taskstr set shfl_done_mappers [list]
		$taskstr set sort_done_mappers 0
		$taskstr set mchain [new MRPerf/Thread/MultiChain $no_copiers]
		#$taskstr set mchain [new MRPerf/Thread/MultiChain $num_of_nodes]
		$taskstr set in_mem_queue [new MRPerf/Queue]
		$taskstr set local_fs_queue [new MRPerf/Queue]
		$taskstr set copier_count 0
		$taskstr set job $job

		set app2 [$self new-connection $node]
		$app2 set job $job
		$app2 snd "reduce task $taskstr"
		puts [$job set stats] "Task TASKID=\"$taskstr\" TASK_TYPE=\"REDUCE\" START_TIME=\"[$self now]\" SPLITS=\"\""
		$node set running_reducetasks [expr [$node set running_reducetasks] + 1]
		$taskstr set expire_timer [new MRPerf/Util/Timer $::timers(task_dead) "$taskstr fail"]

		$job set no_reducetasks_to_start [expr [$job set no_reducetasks_to_start] - 1]
		#upvar #0 no_reducetasks_to_start r
		#set r [expr $r-1]
		#puts "reduce tasks to start $r"
		return 1
	}
	delete $taskstr
	return 0
}

#source timer.tcl
#source shuffle.tcl

if {0} {
proc K {x y} {return $x}

proc lremove {alist toremove} {
	upvar $alist lst
	#puts "to remove $toremove from \[$lst\]"
	set index [lsearch $lst $toremove]
	if {$index >= 0} {
		#puts "did remove"
		set lst [lreplace [K $lst [set lst {}]] $index $index]
	}
	#puts $lst
	#return $alist
}}

MRPerf/NodeApp instproc recv {data} {
	#$self instvar parent
	#global ns
	#sched n1 n buffer_size
	global total_time 
	#num_chunks
	#num_of_reducers
	#heart_beat_delay
	#global tasks_to_start
	#no_maptasks_to_start no_reducetasks_to_start
	#finished_maptasks started_reducers done_reducers
	#dist_cache_size
	#global timers

	set thisnode [$self set hnode]
	set remotenode [[$self dst] set hnode]
	#$::ns trace-annotate "[$thisnode nodename] received data \"$data\" from [$remotenode nodename]"
	#$self out1 "$self ([$self set hnode]) recv from [$self dst] ([[$self dst] set hnode]) \"$data\""

	# 'at' timer reset
	$self set time 0
	set job [$self set job]

	if {[string first "FS request" $data] == 0} {
		# Namenode receives
		[$self nsnode] color cyan
		set job [string range $data 11 end]
		$self inserts "$self snd \"FS reply $job\""

	} elseif {[string first "FS reply" $data] == 0} {
		# Job Tracker receives
		[$self nsnode] color tan

		set job [string range $data 9 end]
		#set no_maptasks_to_start $num_chunks
		#set no_reducetasks_to_start $num_of_reducers

	} elseif {[string first "heartbeat" $data] == 0} {
		# Job Tracker receives
		set remote_node [[$self dst] set hnode]

		set once $::tasks_to_start
		set min $once

		#$self out1 "$remote_node map: $once";# $no_maptasks_to_start"
		#if {$min > [$job set no_maptasks_to_start]} {
		#	set min [$job set no_maptasks_to_start]
		#}
		if {$min > 0} {
			#$self out1 "start $min map tasks on heartbeat"
		}

		set change 0
		if {$::last_total_running != $::total_running} {
			set change 1
		}

		set slots [expr [$::sched max_mappers]-[$remote_node running_maptasks]]
		if {$slots <= 0} {
			#puts "[$self now]: $remote_node running too many ([$remote_node running_maptasks]) maptasks"
		} else {
			$::sched schedule-algorithm $remote_node
			for {set i 0} {$i<$slots} {incr i} {
				set result [$self run_maptask $remote_node]
				#puts "[$self now] run_maptask $remote_node: $result"
				if {$result == 0} {
					break
				}
				incr ::total_running
			}
		}

		if {$change == 1} {
			#puts "[$self now]: $::last_total_running running out of $::last_total_tasks ($remote_node)"
			puts "[$self now]: $::total_running running out of $::total_tasks ($remote_node)"
		}
		set ::last_total_running $::total_running
		set ::last_total_tasks $::total_tasks


		#set once [expr $once - $min]
		set min $once
		#set no_maptasks_to_start [expr $no_maptasks_to_start - $min]

		#$self out1 "$remote_node reduce: $min ";#$no_reducetasks_to_start"
		#if {$min > [$job set no_reducetasks_to_start]} {
		#	set min [$job set no_reducetasks_to_start]
		#}
		for {set i 0} {$i < $min} {incr i} {
			incr once [expr -[$self start_reducetask $remote_node]]
		}
		#set no_reducetasks_to_start [expr $no_reducetasks_to_start - $min]
		#puts $once

		$self inserts "$self snd \"hb reply\""

#	} elseif {[string first "hb reply" $data] == 0} {
		# data nodes receive
#		$::ns after $heart_beat_delay "$self snd \"heartbeat\""

	} elseif {[string first "alive" $data] == 0} {
		set task [string range $data 6 end]
		# task could be either map or reduce
		[$task set expire_timer] resched

		if {[$task info vars mchain] != ""} {
			puts "[$self now]: alive from reduce $task"
		} else {
			puts "[$self now]: alive from map $task"
		}

	} elseif {[string first "map task" $data] == 0} {
		# mappers receive. Note that one node can have multiple mappers
		set task [string range $data 9 end]
		puts "[$self now]: $task map task start [$task chunk] [$task worker] [[$task disk] host]"
		$self set maptask $task
		$self set job [$task set job]
		set job [$self set job]
		puts [$job set stats] "MapAttempt TASKID=\"$task\" TASK_ATTEMPT_ID=\"$self\" START_TIME=\"[$self now]\" HOSTNAME=\"[[$task worker] hostname]\""


		#$task set alive_timer [$::ns after $::timers(alive) "$self send_alive $task"]
		$task set alive_timer [new MRPerf/Util/AliveTimer $::timers(alive) "$self send_alive $task"]


		set node [$self set hnode]

		if {[$node info vars next_task_fail] != ""} {
			puts "$task is set to fail"
			$node unset next_task_fail
			$task set to_fail 1
		} elseif {[$node info vars next_task_passive_fail] != ""} {
			puts "$task is set to passively fail"
			$node unset next_task_passive_fail
			$task set to_passive_fail 1
		}

		#puts "map task started on $node"
		#$self inserts "$task add-counter init"
		$self jvm-start $task
		$self inserts "$task add-counter map_start"

		set chunkstr [$task chunk]
		set alist [split $chunkstr ":"]
		set length [lindex $alist 3]
		$task set input_size $length


		#puts "[[$task disk] host] [$task worker]"
		if { [[$task disk] host] == [$task worker] } {
			[$self nsnode] color green

			for {set remain $length} {$remain > 0} {} {
				set offset [expr $length-$remain]

				set fr [[$job set filter_ratio] value]
				set input_size [expr { $remain > [$job set buffer_size]/$fr ? [$job set buffer_size]/$fr : $remain}]
				set output_size [expr $input_size * $fr]

				$self readsize $task $offset $input_size
				$self compute $task $offset $input_size
				$self inserts "$task add-counter map_finish"
				$self spill $task $output_size
				set remain [expr $remain - $input_size]
			}
			if {[$task set spill] > 1} {
				$self merge $task
			}
			$self inserts "$self snd \"map finish $task\""
		} else {
			# wanggy 2009.6.9
			# Problem: if the data node has already failed, the
			# scheduler will not know the failure, and still
			# schedule a rack-local or rack-remote task to get
			# data from that data node. That map task will
			# never finish, but because it sends out keep-alive
			# it's never killed, and will run forever.
			# ------------------
			# possible work around: add a data_req_timer=189
			# This is still not correct. The current implementation
			# requires whole data reply by 189s. If due to slow
			# network, data reply is started but not finished by
			# 189s, the task will fail. But the correct behavior
			# is to wait for the task to finish, as long as the
			# request reaches data node. TCP is another layer.
			# -----------------
			# Preferably, send function will return an error,
			# after 189s, to the sender. That can be an event.

			# TODO: what if data node fails, and "data request"
			# never comes back. Scheduler should know about
			# which nodes have failed, and avoid scheduling
			# to those nodes.
			# TODO What about inter-rack link failure?

			#puts [$task chunk]
			#puts "[[$task disk] host] [$task worker]"
			[$self nsnode] color red
			#filter_ratio
			set fr [[$job set filter_ratio] value]
			set size [expr { $length > [$job set buffer_size]/$fr ? [$job set buffer_size]/$fr : $length}]

			set datanode [[$task disk] host]
			set app4 [$self new-connection2 $datanode]
			$app4 set fr $fr
			#$task set data_req_timer [$::ns after $::timers(data_req) "$task fail"]
			$app4 set task $task
			$task set connection $app4
			$self inserts "$app4 snd \"data request $task 0 $size\""
			#set app5 [$app4 dst]
			#$app4 send 100 "[$app4 dst] recv {data request $task}"
		}

	} elseif {[string first "data request" $data] == 0} {
		# Data nodes receive. In hadoop it should be Task Tracker that
		# receives this message. But here it's a temporary NodeApp.
		[$self nsnode] color blue
		set alist [split $data]

		set task [lindex $alist 2]
		set offset [lindex $alist 3]
		set size [lindex $alist 4]

		set chunkstr [$task chunk]
		set alist [split $chunkstr ":"]
		set length [lindex $alist 3]

		if { [$self set hnode] == [[$task disk] host] } {
			#$self enqueue

			$self readsize $task $offset $size
			#puts "inserts $self send $size ......."
			$self inserts "$self send $size \"[$self dst] recv {data reply $task $offset $size}\""
		} else {
			## error!
			puts [[$self set hnode] nodename]
			puts "Error! data requested not on this node"
			#$::ns trace-annotate "Error! data requested not on this node"
		}

	} elseif {[string first "data reply" $data] == 0} {
		# mappers receive
		[$self nsnode] color green
		set alist [split $data]

		set task [lindex $alist 2]
		set offset [lindex $alist 3]
		set size [lindex $alist 4]
		set output_size [expr $size * [$self set fr]]

		set chunkstr [$task chunk]
		set alist [split $chunkstr ":"]
		set length [lindex $alist 3]

		$self compute $task $offset $size
		$self inserts "$task add-counter map_finish"

		$self spill $task $output_size

		if {[expr $offset + $size >= $length]} {
			if {[$task set spill] > 1} {
				$self merge $task
			}

			set app6 [$self set parent]
			$self inserts "$app6 snd \"map finish $task\""
		} else {
			#filter_ratio
			set fr [[$job set filter_ratio] value]
			set r [expr $length - $offset]
			set nextsize [expr { $r > [$job set buffer_size]/$fr ? [$job set buffer_size]/$fr : $r}]
			set nextoffset [expr $offset + $size]
			$self set fr $fr
			$self inserts "$self snd \"data request $task $nextoffset $nextsize\""
		}

	} elseif {[string first "map finish" $data] == 0} {
		# Job Tracker receives
		set task [string range $data 11 end]
		set worker [[$self dst] set hnode]
		if {[$task info vars to_fail] != ""} {
			$task unset to_fail
			$task fail
			return
		} elseif {[$task info vars to_passive_fail] != ""} {
			# TODO: wanggy 2009-6-9
			# this is not implemented yet, since it's not used.
			$task unset to_passive_fail
			return
		}

		puts "[$self now]: $task map finish [$task worker] [$task chunk]"


		#$::ns cancel [$task set alive_timer]
		[$task set alive_timer] cancel
		delete [$task set alive_timer]
		$task unset alive_timer


		#$::ns cancel [[$task set expire_timer] set eid]
		[$task set expire_timer] cancel
		delete [$task set expire_timer]
		$task unset expire_timer


		#puts "maptask $task of $job finishes, to write to [$job set stats]"
		puts [$job set stats] "MapAttempt TASKID=\"$task\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"[$self now]\" HOSTNAME=\"[[$task worker] hostname]\""

		set chunkstr [$task chunk]
		set alist [split $chunkstr ":"]
		set filename [lindex $alist 0]
		set chunk [lindex $alist 1]
		#$::ns trace-annotate "***** Chunk $filename:$chunk finished on [$remotenode nodename] *****"

		#$self wrap-up $task

		$task set finish_time [$self now]
		$task write-counters


		if {1} {
			set reduceapp [[$job set started_reducers] first]
			while {$reduceapp != ""} {
				$self inserts "[$reduceapp dst] snd \"map f1inish signal $task\""
				set reduceapp [[$job set started_reducers] ne1xt]
			}
		} else {
			foreach reduceapp $started_reducers {
				$self inserts "[$reduceapp dst] snd \"map f1inish signal $task\""
				#puts "Job Tracker sent map $task f1inish signal to $reduceapp"
			}
		}
		$task finish
		incr ::total_tasks -1
		incr ::total_running -1
		if {1} {
			[$job set finished_maptasks] insert $task
			set remain [expr [$job set num_chunks] - [[$job set finished_maptasks] size]]
			if {$remain == 0} {
				puts "[$self now]: Job $job all map tasks finished"
			} elseif {$remain < 0} {
				puts "error, remain == $remain"
			}
		} else {
			$job instvar finished_maptasks
			lappend finished_maptasks $task
		}

		#$self inserts "[$self set parent] start_maptask $worker"

	} elseif {[string first "reduce task" $data] == 0} {
		# reducers receive. One node can have multiple reducers.
		set rtask [string range $data 12 end]
		$self out1 "reducer($rtask) started on node([$rtask worker])"

		$self set reducetask $rtask
		$rtask set app $self
		$self set job [$rtask set job]
		set job [$self set job]
		puts [$job set stats] "ReduceAttempt TASKID=\"$rtask\" TASK_ATTEMPT_ID=\"$self\" START_TIME=\"[$self now]\" HOSTNAME=\"[[$rtask worker] hostname]\""


		#$rtask set alive_timer [$::ns after $::timers(alive) "$self send_alive $rtask"]
		$rtask set alive_timer [new MRPerf/Util/AliveTimer $::timers(alive) "$self send_alive $rtask"]


		# potentially increasing counter here is not correct.
		# Another heartbeat may goes to job tracker, even before this counter
		# gets updated.
		set node [$self set hnode]

		if {[$node info vars next_reduce_task_fail] != ""} {
			puts "reduce task $rtask is set to fail"
			$node unset next_reduce_task_fail
			$rtask set to_fail 1
		} elseif {[$node info vars next_reduce_task_passive_fail] != ""} {
			puts "$rtask is set to passively fail"
			$node unset next_reduce_task_passive_fail
			$rtask set to_passive_fail 1
		}

		$self jvm-start $rtask
		$self inserts "$rtask add-counter reduce_start"

		if {1} {
			set maptask [[$job set finished_maptasks] first]
			while {$maptask != ""} {
				$rtask fetch_map_output $maptask $self
				set maptask [[$job set finished_maptasks] ne1xt]
			}
		} else {
			$job instvar finished_maptasks
			foreach maptask $finished_maptasks {
				$rtask fetch_map_output $maptask $self
			}
		}

		if {1} {
			[$job set started_reducers] insert $self
		} else {
			$job instvar started_reducers
			lappend started_reducers $self

			#if {[llength $started_reducers] == $num_of_reducers} {
			#	$self inserts "lreplace started_reducers 0 0"
			#}
		}

	} elseif {[string first "map f1inish signal" $data] == 0} {
		# reducers receive
		set alist [split $data]
		set task [lindex $alist 3]
		set rtask [$self set reducetask]

		#puts "reduce task $rtask"
		#puts "map $task f1inish signal to reduce task $rtask received at $self"

		$rtask fetch_map_output $task $self

	} elseif {[string first "ghost map" $data] == 0} {
		set alist [split $data]
		set ghost_map [lindex $alist 2]
		puts "[$self now]: ghost map $ghost_map found by $self"

		$ghost_map mark_ghost

	} elseif {[string first "reduce data request" $data] == 0} {
		# data nodes receive. What is requested is intermediate result,
		# output previously by mappers
		set alist [split $data]
		set rcopier [lindex $alist 3]

		set app20 [$self new-connection2 $remotenode]
		[$app20 dst] set rcopier $rcopier
		$rcopier set heartbeat_app $app20


		#$app20 shuffle_send_alive
		$app20 set shuffle_alive [new MRPerf/Util/AliveTimer $::timers(alive) "$app20 shuffle_send_alive"]


		# Guanying 2009.6.12 workaround
		# When output size is small, sometimes [$rcopier set size] can be 
		# less than 1.0, and "$self send" will not do anything, so the other
		# end assume the message is lost and a ghost map is found.
		# workaround: limit size minimum to 10.0. However, the better way to
		# fix the problem is to limit minimum in ns-2 C++ code.
		set size [$rcopier set size]
		if {$size < 10.0} {
			set size 10.0
		}
		#TODO: maybe specify offset instead of 0 here
		$self readsize [$rcopier set maptask] 0 [$rcopier set size]
		$self inserts "$self send $size \"[$self dst] recv {reduce data reply $rcopier}\""

	} elseif {[string first "shuffle heartbeat" $data] == 0} {
		# reduce copiers receive


		#set rcopier [$self set rcopier]
		$self instvar rcopier


		if {[$rcopier set done] == 0} {
			[$rcopier set ghost_map_timer] resched
			#$rcopier set ghost_map_timer [$::ns after $::timers(ghost_map) "$rcopier mark_ghost"]
		}

	} elseif {[string first "reduce data reply" $data] == 0} {
		# reducers receive
		set alist [split $data]
		set rcopier [lindex $alist 3]
		set rtask [$rcopier set rtask]

		#$::ns cancel [[$rcopier set heartbeat_app] set shuffle_heartbeat_event]
		[[$rcopier set heartbeat_app] set shuffle_alive] cancel
		delete [[$rcopier set heartbeat_app] set shuffle_alive]
		[$rcopier set heartbeat_app] unset shuffle_alive


		[$rcopier set ghost_map_timer] cancel
		delete [$rcopier set ghost_map_timer]
		$rcopier unset ghost_map_timer


		# TODO: delete connection
		$rcopier set done 1

		#set c [[$rtask set counters] set copiers([$rcopier set count])]
		#$self inserts "$rtask add1-counter $c copier_finish"

		[$rtask set mchain] next-task
		#$self inserts "puts [[$rtask set mchain] size]"

		$self reduce-merge-sort $rcopier
		#puts "$rcopier, [$rcopier set maptask]"
		$self inserts "$rtask shuffle-finish [$rcopier set maptask] $self"
		$self inserts "$self is-sort-finished $rtask"

	} elseif {[string first "reduce finish" $data] == 0} {
		# Job Tracker receives
		set alist [split $data]
		set rtask [lindex $alist 2]

		set task $rtask
		if {[$task info vars to_fail] != ""} {
			$task unset to_fail
			$task fail
			return
		} elseif {[$task info vars to_passive_fail] != ""} {
			# TODO: wanggy 200-6-9
			# this is not implemented, since it's not used
			$task unset to_passive_fail
			return
		}


		#$::ns cancel [$rtask set alive_timer]
		[$rtask set alive_timer] cancel
		delete [$rtask set alive_timer]
		$rtask unset alive_timer


		#$::ns cancel [[$task set expire_timer] set eid]
		[$task set expire_timer] cancel
		delete [$rtask set expire_timer]
		$rtask unset expire_timer

		set c [$rtask set counters]
		puts [$job set stats] "ReduceAttempt TASKID=\"$rtask\" TASK_ATTEMPT_ID=\"[$self dst]\" TASK_STATUS=\"SUCCESS\" HOSTNAME=\"[[$rtask worker] hostname]\" SHUFFLE_FINISHED=\"[$c set shuffle_finished]\" SORT_FINISHED=\"[$c set sort_finished]\" FINISH_TIME=\"[$self now]\""
		$rtask write-counters

		$rtask set finish_time [$self now]

		set worker [[$self dst] set hnode]
		$worker set running_reducetasks [expr [$worker set running_reducetasks] - 1]

		$job set done_reducers [expr [$job set done_reducers] + 1]
		$self out1 "([$job set done_reducers] / [$job set num_of_reducers]) reducers done"

		if {[$job set no_reducetasks_to_start] > 0} {
			$self inserts "$self start_reducetask $worker"
		}

		if {[$job set done_reducers] > [$job set num_of_reducers]} {
			puts "error 2"
		} elseif {[$job set done_reducers] == [$job set num_of_reducers]} {
			set total_time [$::ns now]
			$job finish

		}

	}

}

set namfile	out.nam
set tracefile	out.tr



# define color index
$ns color 0 black
$ns color 1 blue
$ns color 2 cyan
$ns color 3 yellow
$ns color 4 brown
$ns color 5 tan
$ns color 6 gold
$ns color 7 red
$ns color 8 purple
$ns color 9 green



set total_time 0


