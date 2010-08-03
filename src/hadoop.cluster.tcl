source hadoop.scheduler.tcl
#puts "sched constructed"
set tasks_to_start 2
	# number of tasks to start at one heartbeat
set finish_time 5000.0

set read_seek 0;#0.0082
set write_seek 0;#0.0092

set timers(alive) 30
set timers(task_dead) 600
set timers(ghost_map) 189
set timers(data_req) 189

#set avg_record_size 100
	# in byte.
	# not in use --Guanying 2008.1.11
#set jt $n_rg0_0_ng0_1

set heart_beat_delay 1

# mappers
$sched max_mappers 2
	# per node
# reducers
set max_reducers 1
	# concurrent reducers per node.


