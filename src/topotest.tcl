

set ns [new Simulator]
source common.tcl

source io.tcl
source timer.tcl
source map.tcl
source reduce.tcl
source shuffle.tcl

# open trace files and enable tracing
set nf [open $namfile w]
#$ns namtrace-all $nf
set f [open $tracefile w]
#$ns trace-all $f

set stats [open $statsfile w]

source "common-schedule.tcl"
source "schedule.tcl"
source "common-topology.tcl"
source "hadoop.topo.tcl"

set mapnodes [list]
set finished 0
set heart_beat_delay 0.2
set max_time 10000


source "topo-parameters.tcl"


MRPerf/NodeApp instproc recv {data} {
	global mapnodes round size finished data_nodes ns total_time stats
	set remote_node [[$self dst] set hnode]
	#puts "[$ns now]: $self recv \"$data\" from [$self dst]"

	if {[string first "heartbeat" $data] == 0} {
		# Job Tracker receives
		if {[lsearch $mapnodes $remote_node] != -1} {
			return
		}

		foreach mn $mapnodes {
			set app1 [$self new-connection $mn]
			$self inserts "$app1 snd \"ready $remote_node\""
		}
		lappend mapnodes $remote_node

	} elseif {[string first "ready" $data] == 0} {
		set alist [split $data]
		set newnode [lindex $alist 1]
		set app2 [$self new-connection $newnode]
		$self inserts "$app2 snd \"first-req $round\""

	} elseif {[string first "first-req" $data] == 0} {
		set alist [split $data]
		set curround [lindex $alist 1]
		$self inserts "$self send $size \"[$self dst] recv {reply $curround}\""

		set app3 [$self new-connection $remote_node]
		$self inserts "$app3 snd \"req $round\""

	} elseif {[string first "reply" $data] == 0} {
		set alist [split $data]
		set curround [lindex $alist 1]
		if {$curround-1 > 0} {
			$self inserts "$self snd \"req [expr $curround-1]\""
		} else {
			incr finished
			if {$finished % $data_nodes == 0} {
				puts $stats "[$ns now]: $finished finished"
			}
			if {$finished >= ($data_nodes-1)*$data_nodes} {
				set total_time [$ns now]
				puts $stats "finished at $total_time"
				finish
			}
		}

	} elseif {[string first "req" $data] == 0} {
		set alist [split $data]
		set curround [lindex $alist 1]
		$self inserts "$self send $size \"[$self dst] recv {reply $curround}\""

		if {$curround == $round} {
			#puts "[$ns now] reverse request"
		}
	}
}

$ns at $max_time "finish"

$ns run

