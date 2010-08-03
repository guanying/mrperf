
set g_rng [new RNG]
#$g_rng seed [$ns now]
$g_rng seed 25134

set g_random [new RandomVariable/Uniform]
$g_random set min_ 0
$g_random set max_ 2147483647
$g_random use-rng $g_rng

proc rand1 {value} {
	#global g_random
	return [expr [$::g_random value] / 2147483647. * $value]
}

set g_exp [new RandomVariable/Exponential]
set g_lnormal [new RandomVariable/LogNormal]
$g_lnormal use-rng $g_rng

proc rand2 {value} {
	#global g_lnormal
	#puts $value
	$::g_lnormal set avg_ [expr log($value)]
	$::g_lnormal set std_ [expr log(1.10)]
	set result [$::g_lnormal value]
	#puts "rand2($value) = $result"
	return $result
}

proc rand3 {avg stdev} {
	#global g_lnormal
	$::g_lnormal set avg_ [expr log($avg)]
	$::g_lnormal set std_ [expr log($stdev)]
	set result [$::g_lnormal value]
	return $result
}

# append a device task to the end of the chain
MRPerf/NodeApp instproc add-dev1ice-task {units device} {
	#puts "************ $units *************"
	set task [new MRPerf/Thread/Task]
	$task conf [rand2 $units] "[$device set tasklist] insert $task" 1
	[$self set chain] attach $task
}

# append a simple task (command) to the end of the chain
MRPerf/NodeApp instproc add-sim1ple-task {command} {
	set task [new MRPerf/Thread/Task]
	$task conf 0 $command 0
	[$self set chain] attach $task
}

# insert a device task to the current position of the chain
# used for expanding a function to specific commands
MRPerf/NodeApp instproc insertd {units device} {
	#puts "************ $units *************"
	set task [new MRPerf/Thread/Task]
	$task conf [rand2 $units] "[$device set tasklist] insert $task" 1
	[$self set chain] insert $task
}

MRPerf/NodeApp instproc insert1d {units stdev device} {
	#puts "************ $units *************"
	#puts "This function should not be called"
	set task [new MRPerf/Thread/Task]
	$task conf [rand3 $units $stdev] "[$device set tasklist] insert $task" 1
	[$self set chain] insert $task
}

MRPerf/NodeApp instproc ioread {bytes disk} {
	#global read_seek
	set units [expr 1.0*$bytes + $::read_seek * [$disk set rbw]]
	$self insertd $units $disk
}

MRPerf/NodeApp instproc iowrite {bytes disk} {
	#global write_seek
	set wbw [$disk set wbw]
	set rbw [$disk set rbw]
	# units = (bytes+write_seek*wbw)*rbw/wbw
	set units [expr 1.0*$bytes * $rbw/$wbw + $::write_seek * $rbw]
	$self insertd $units $disk
}

# insert a simple task (command) to the current position of the chain
# used for expanding a function to specific commands
MRPerf/NodeApp instproc inserts {command} {
	set task [new MRPerf/Thread/Task]
	#$task conf 0 "puts {tcl time took \"$command\": [time $command]}" 0
	$task conf 0 $command 0
	[$self set chain] insert $task
}

