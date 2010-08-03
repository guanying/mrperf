

proc newnodealone {nodename} {
	#global nodename
	global n30 ns allnode
	upvar $nodename local
	set n30 [set local [new MRPerf/Node]]
	set nsnode [$ns node]
	$n30 setnodename $nodename
	$n30 set nsnode $nsnode
}

proc newnode {nodename router {gyaddr ""}} {
	global n30 ns allnode int_bw int_latency
	upvar #0 $nodename local
	set n30 [set local [new MRPerf/Node]]
	if {$gyaddr == ""} {
		set nsnode [$ns node]
	} else {
		set nsnode [$ns node $gyaddr]
	}
	$nsnode shape square
	$n30 setnodename $nodename
	$n30 set nsnode $nsnode
	$n30 set running_maptasks 0
	$n30 set running_reducetasks 0
	$n30 set uncounted 0
	$ns duplex-link $router $nsnode $int_bw $int_latency DropTail
}

MRPerf/Node instproc newdisk { rbw wbw {num 1} } {
	set disk [new MRPerf/Node/Disk]
	$self add-disk $disk
	$disk set rbw $rbw
	$disk set wbw $wbw
	$disk set tasklist [new MRPerf/TaskList $rbw $num]
}

MRPerf/Node instproc hostname {} {
	set rack [$self rack]
	# 1 from "rg0_1"
	return "/rack_$rack/Node_[string range $self 2 end]"
	# /rack_1/Node_37 if this object is _o37
}

RtModule/Manual instproc add1route { dst_address next_hop } {
	set ns [Simulator instance]
	set target_node $next_hop
	set link [$ns link [$self node] $target_node]
	set target [$link head]
	return [$self add-route $dst_address $target]
}

proc set_mapnodes {racks nodes data_nodes} {
	global ns jt
	for {set i 0} {$i < $racks && $i*$nodes < $data_nodes} {incr i} {
		for {set j 0} {$j < $nodes && $i*$nodes+$j<$data_nodes} {incr j} {
			upvar #0 n_rg0_$i\_ng0_$j mn
			set tcp0 [new Agent/TCP/FullTcp]
			set dummy [new MRPerf/NodeApp $tcp0]
			$dummy set hnode $mn
			set app11 [$dummy new-connection $jt]


			$app11 set heartbeat_timer [new MRPerf/Util/AliveTimer $::heart_beat_delay "$app11 send_heartbeat"]
			#$ns at 0.05 "$app11 send_heartbeat"


		}
	}
}

proc setup_2level_dcell {dcell_n} {
	global ns cpu_freq cpu_cores rbw wbw
	$ns rtproto Manual
	for {set j 0} {$j < [expr $dcell_n+1]} {incr j} {
		global r_rg0_$j
		set r [set r_rg0_$j [$ns node]]
		for {set i 0} {$i < $dcell_n} {incr i} {
			newnode "n_rg0_$j\_ng0_$i" $r
			global n30
			$n30 set tasklist [new MRPerf/TaskList $cpu_freq $cpu_cores]
			for {set k 0} {$k < 1} {incr k} {
				$n30 newdisk $rbw $wbw
			}
			if {$j > $i} {
				# j,i --- i,j-1
				set node1 [format "n_rg0_%s_ng0_%s" $j $i]
				set node2 [format "n_rg0_%s_ng0_%s" $i [expr $j-1]]
				upvar #0 $node1 n1
				upvar #0 $node2 n2
				$ns duplex-link [$n1 set nsnode] [$n2 set nsnode] 1Gb 0.15ms DropTail
			}
		}
	}

if {1} {
	# set route
	for {set j 0} {$j < [expr $dcell_n+1]} {incr j} {
		for {set i 0} {$i < $dcell_n} {incr i} {
			if {$j > $i} {
				# j,i --- i,j-1
				upvar #0 n_rg0_$j\_ng0_$i n1
				upvar #0 "n_rg0_$i\_ng0_[expr $j-1]" n2
				set rt [[$n1 set nsnode] get-module "Manual"]
				for {set k 0} {$k < $dcell_n} {incr k} {
					upvar #0 n_rg0_$i\_ng0_$k n3
					if {$k == $j-1} {
						$rt add-route-to-adj-node [$n3 set nsnode]
					} else {
						$rt add1route [[$n3 set nsnode] id] [$n2 set nsnode]
					}
					#puts "[[$n1 set nsnode] id] -> [[$n3 set nsnode] id] via [[$n2 set nsnode] id]"
				}
				upvar #0 r_rg0_$j router
				$rt add-route-to-adj-node -default $router
				#puts "[[$n1 set nsnode] id] -> [$router id] via [$router id]"
			} else {
				# j,i --- i+1,j
				upvar #0 n_rg0_$j\_ng0_$i n1
				upvar #0 "n_rg0_[expr $i+1]\_ng0_$j" n2
				set rt [[$n1 set nsnode] get-module "Manual"]
				for {set k 0} {$k < $dcell_n} {incr k} {
					upvar #0 "n_rg0_[expr $i+1]\_ng0_$k" n3
					if {$k == $j} {
						$rt add-route-to-adj-node [$n3 set nsnode]
					} else {
						$rt add1route [[$n3 set nsnode] id] [$n2 set nsnode]
					}
					#puts "[[$n1 set nsnode] id] -> [[$n3 set nsnode] id] via [[$n2 set nsnode] id]"
				}
				upvar #0 r_rg0_$j router
				$rt add-route-to-adj-node -default $router
				#puts "[[$n1 set nsnode] id] -> [$router id] via [$router id]"
			}
		}
	}

	for {set j 0} {$j < [expr $dcell_n+1]} {incr j} {
		upvar #0 r_rg0_$j router
		set rt [$router get-module "Manual"]
		upvar #0 "n_rg0_$j\_ng0_0" n
		$rt add-route-to-adj-node -default [$n set nsnode]
		#puts "[$router id] -> [[$n set nsnode] id] via [[$n set nsnode] id]"
		for {set i 1} {$i < $dcell_n} {incr i} {
			upvar #0 n_rg0_$j\_ng0_$i n
			$rt add-route-to-adj-node [$n set nsnode]
			#puts "[$router id] -> [[$n set nsnode] id] via [[$n set nsnode] id]"
		}
		for {set k 0} {$k < $j} {incr k} {
			upvar #0 n_rg0_$j\_ng0_$k n2
			for {set i 0} {$i < $dcell_n} {incr i} {
				upvar #0 n_rg0_$k\_ng0_$i n1
				$rt add1route [[$n1 set nsnode] id] [$n2 set nsnode]
				#puts "[$router id] -> [[$n1 set nsnode] id] via [[$n2 set nsnode] id]"
			}
			upvar #0 r_rg0_$k router2
			$rt add1route [$router2 id] [$n2 set nsnode]
			#puts "[$router id] -> [$router2 id] via [[$n2 set nsnode] id]"
		}
		for {set k $j} {$k < $dcell_n} {incr k} {
			upvar #0 n_rg0_$j\_ng0_$k n2
			for {set i 0} {$i < $dcell_n} {incr i} {
				upvar #0 "n_rg0_[expr $k+1]\_ng0_$i" n1
				$rt add1route [[$n1 set nsnode] id] [$n2 set nsnode]
				#puts "[$router id] -> [[$n1 set nsnode] id] via [[$n2 set nsnode] id]"
			}
			upvar #0 "r_rg0_[expr $k+1]" router2
			$rt add1route [$router2 id] [$n2 set nsnode]
			#puts "[$router id] -> [$router2 id] via [[$n2 set nsnode] id]"
		}
	}
}

}


