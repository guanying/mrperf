
set ns [new Simulator]

source common.tcl

source io.tcl
source timer.tcl
source map.tcl
source reduce.tcl
source shuffle.tcl

# open trace files and enable tracing
#set nf [open $namfile w]
#$ns namtrace-all $nf
#set f [open $tracefile w]
#$ns trace-all $f


source common-schedule.tcl
source schedule.tcl
source common-topology.tcl
puts begin


source hadoop.cluster.tcl
#puts "cluster"

source hadoop.topo.tcl
#puts "topology created"

$sched set_jt $jt


#puts "$jt $nn"
#puts "scheduler created"


source hadoop.job.tcl


proc init_job {job} {
	#global sched
	set data_file [format "job_%s_data.xml" [$job job_id]]
	$job set num_chunks [$::sched read2metadata $data_file $job]

	#$job set num_of_reducers [expr $::max_reducers*$::num_of_nodes]
		# total reducers to start
	$job set num_of_reducers [expr int([rand1 [expr [$job set num_chunks]/5]])]
	if {[$job set num_of_reducers] < 1} {
		$job set num_of_reducers 1
	}

	$job set finished_maptasks [new MRPerf/Util/Set]
	$job set started_reducers [new MRPerf/Util/Set]

	# new scheduler on multiple jobs keeps task count in C++
	#$job set no_maptasks_to_start [$job set num_chunks]
	$job set no_reducetasks_to_start [$job set num_of_reducers]

	$job set done_reducers 0

	$job set stats_file [format "stats_%s" [$job job_id]]
	$job set stats [open [$job set stats_file] w]
}



set next_job_id 1
set job_queue [new MRPerf/Thread/MultiChain 1]

#puts "NN and JT created"


#set uncounted 0


set total_tasks 0
set total_running 0
set last_total_tasks 0
set last_total_running 0

proc submit_job {submit_time type args} {
	#global ns
	global next_job_id
	#global job_queue
	global trace_length

	set job [new MRPerf/Job]
	#set args [lindex $args 0]
	#puts "init_$type $job $args"
	if {$args == ""} {
		init_$type $job
	} else {
		init_$type $job $args
	}
	$job set_job_id $next_job_id
	incr next_job_id
	init_job $job
	$job set_submit_time $submit_time

	$job instvar stats
	set job_id [$job job_id]
	puts $stats "Job JOBID=\"job_sim_$job_id\" JOBNAME=\"$type-sim\" USER=\"mrperf\" SUBMIT_TIME=\"$submit_time\" JOBCONF=\"job.xml\""

	# Guanying 2009.10.8
	#   In previous code, when a job is started, JobTracker first talks to
	# NameNode, and after that start to distribute tasks.
	#   Now multiple jobs are supported, so jobs are put in queue before
	# they start. At which point a new job submitted is put in the queue is
	# debatable. Either a job is put into the queue at submission, or it can
	# be put in queue after JobTracker has talked to NameNode. In the latter
	# case, talking can be counted as part of submission process, and it
	# would become similar as the first case. So I remove the talking process,
	# and put a new job into queue right after it is submitted.
	if {1} {
		# 2009.10.15 start job when it's submitted.
		#$::ns at $submit_time "puts \"Job $job is submitted at $submit_time\"; $::job_queue attach $job"
		$::ns at $submit_time "puts \"Job $job is submitted at $submit_time\"; incr ::total_tasks [$job set num_chunks]; $job run"
		puts $stats "Job JOBID=\"job_sim_$job_id\" LAUNCH_TIME=\"$submit_time\" TOTAL_MAPS=\"[$job set num_chunks]\" TOTAL_REDUCES=\"[$job set num_of_reducers]\""
		# 2009.10.15 done
	} else {
		set tcp0 [new Agent/TCP/FullTcp]
		set dummy [new MRPerf/NodeApp $tcp0]
		$dummy set hnode $jt
		#$dummy set job $job
		set app1 [$dummy new-connection $nn]

		# send a message via MRPerf/NodeApp
		# The string will be interpreted by the receiver as Tcl code.
		$::ns at $submit_time "$app1 snd {FS request $job}"
	}

	set trace_length $submit_time
}

set jobs_list [list]

Class MRPerf/JobSubmission

MRPerf/JobSubmission instproc init {submit_time type args} {
	$self set submit_time $submit_time
	$self set type $type
	$self set args $args
}

proc new_job {submit_time type args} {
	lappend ::jobs_list [new MRPerf/JobSubmission $submit_time $type $args]
}

source job.trace.tcl

foreach job $jobs_list {
	#puts [$job set args]
	set args [lindex [$job set args] 0]
	#puts $args
	if {[$job set args] == "{}" } {
		submit_job [$job set submit_time] [$job set type]
	} else {
		submit_job [$job set submit_time] [$job set type] $args
	}
}

#[$nn set nsnode] label NN
#[$jt set nsnode] label JT


#$ns set-animation-rate 0.5ms
#$ns at 0.2 "$ns set-animation-rate 1ms"
#$ns at 0.5 "$ns set-animation-rate 2ms"

$ns at $finish_time "finish"

puts "now start simulation"

$ns run

