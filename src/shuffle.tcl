
Class MRPerf/ReduceCopier

MRPerf/ReduceCopier instproc init {maptask rtask part_size app} {
	$self set maptask $maptask
	$self set rtask $rtask
	$self set size $part_size
	$self set app $app
	$self set done 0
	#$rtask set rcopier $self
}

MRPerf/ReduceCopier instproc run {} {
	set rtask [$self set rtask]
	set size [$self set size]

	$rtask set input_size [expr [$rtask set input_size] + $size]
	set app9 [[$self set app] new-connection2 [[$self set maptask] worker]]

	$app9 set rcopier $self
	$self set connection $app9

	#global ns timers
	$self set ghost_map_timer [new MRPerf/Util/Timer $::timers(ghost_map) "$self mark_ghost"]

	#set c [[$rtask set counters] set copiers([$self set count])]
	#[$self set app] inserts "$rtask add1-counter $c copier_run"
	[$self set app] inserts "$app9 snd \"reduce data request $self\""

}

MRPerf/ReduceCopier instproc mark_ghost {} {
	[[$self set rtask] set mchain] next-task
	[$self set app] snd "ghost map [$self set maptask]"
}

