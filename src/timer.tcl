
Class MRPerf/Timer

MRPerf/Timer instproc init {interval command} {
	global ns
	$self set interval $interval
	$self set command $command
	$self set eid [$ns after $interval "$self expire"]
	#puts "[$ns now]: timer set to after $interval to do /$command/"
}

MRPerf/Timer instproc reset {} {
	global ns
	$ns cancel [$self set eid]
	$self set eid [$ns after [$self set interval] "[$self set command]"]
	#puts "[$ns now]: timer reset to after [$self set interval] to do /[$self set command]/"
}

MRPerf/Timer instproc expire {} {
	global ns
	puts "[$ns now]: $self expire, to eval [$self set command]"
	eval "[$self set command]"
}

