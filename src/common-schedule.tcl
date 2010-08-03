

MRPerf/MapTask instproc data-local-schedule {hnode} {
	$self fixed-schedule $hnode $hnode
}

# rand1 returns a double, rand4 returns an int
proc rand4 {value} {
	return [expr int([rand1 $value])]
}

MRPerf/MapTask instproc rack-local-schedule {worker} {
	global racks nodes data_nodes
	set alist [split [$worker nodename] "_"];# n_rg0_1_ng0_2
	set rid [lindex $alist 2]
	set wnid [lindex $alist 4]

	if {[string first [$self info vars] no_rack_local] != -1} {
		puts "Error: circular invocation. $racks,$nodes,$data_nodes"
	}

	#puts "$rid, $wnid"
	if {$data_nodes == 1} {
		puts "only 1 data node, have to do data-local"
		return data-local-schedule $hnode
	} elseif {$data_nodes - $rid*$nodes == 1} {
		$self set no_rack_local 1
		puts "only 1 node in its rack, have to do rack-remote"
		return rack-remote-schedule $hnode
	}

	# find a rack-local node
	while {1} {
		set hnid [rand4 $nodes]
		if { $rid*$nodes + $hnid >= $data_nodes } {
			# (rid, nid) is out of data nodes
			continue
		}
		if { $hnid != $wnid } {
			# candidate
			break
		}
	}
	upvar #0 n_rg0_$rid\_ng0_$hnid host

	$self fixed-schedule $worker $host
}

MRPerf/MapTask instproc mixed-local-schedule {hnode} {
	if {[rand1 10000] < 5000} {
		# data-local schedule
		$self data-local-schedule $hnode
	} else {
		# rack-local schedule
		$self rack-local-schedule $hnode
	}
}

MRPerf/MapTask instproc rack-remote-schedule {worker} {
	global racks nodes data_nodes
	set alist [split [$worker nodename] "_"];# n_rg0_1_ng0_2
	set rid [lindex $alist 2]
	set wnid [lindex $alist 4]

	if {[string first [$self info vars] no_rack_remote] != -1} {
		puts "Error: circular invocation. $racks,$nodes,$data_nodes"
	}

	if {$racks == 1 || $data_nodes == $nodes} {
		puts "all data nodes in one rack, have to do rack-local"
		$self set no_rack_remote 1
		return rack-local-schedule $worker
	}

	# find a rack-local node
	while {1} {
		set hrid [rand4 $racks]
		set hnid [rand4 $nodes]
		if { $hrid*$nodes + $hnid >= $data_nodes } {
			# (rid, nid) is out of data nodes
			continue
		}
		if { $hrid != $rid } {
			# candidate
			break
		}
	}
	upvar #0 n_rg0_$hrid\_ng0_$hnid host

	$self fixed-schedule $worker $host
}

