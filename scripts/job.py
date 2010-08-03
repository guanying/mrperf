def Uniform(min_, max_):
    return {
                'type' : 'Uniform',
                'min' : min_,
                'max' : max_,
                }

class JobTask:
#---------------------------------------------

    config = {
        'description' : 'Default Description of a job',
        'task_type' : 'normal',
        'topology_file' : 'topology1.xml',
        'config_data' : "Default Configuration Data Used",

	#begin hadoop_conf.py attributes
		'nodes' : ["bom.cs.vt.edu"], #make sure to comma delimit any added nodes

	
		'freq_table' : {
						'Xeon 3.2GHz' : 3.2*1024*1024*1024,
						'Xeon 3.0GHz' : 3.0*1024*1024*1024,
						'Xeon 2.5GHz L5420' : 2.5*1000*1000*1000,
						},
			
		'read_bw_table' : {
							'Seagate' : 280.0*1024*1024, \
							},
        'write_bw_table' : {
							'Seagate' : 75.0*1024*1024,\
							},
        'int_bw' : '1Gb',
        'int_latency' : '0.15ms',
        'ext_bw' :'1Gb',
        'ext_latency' : '0.15ms',
    #end hadoop_conf.py attributes
    #---------------------------------------------
    #begin hadoop.job.tcl attributes
        'jvm_start_cost' : 4.0*1000*1000*1000,
        'wrap_up_cost' : 4.8*1000*1000*1000,

       'io_factor' : 10,
        'in_mem_space' : 100.0*1024*1024 * 0.66,
        'in_mem_segments' : 100,


        #mappers
        'cycles_per_byte' : 20.0,
        'sort_cycles_per_byte' : 44.0,
        'merge_cycles' : 1.0*1000*1000*1000,
       # 'dist_cache_size' : 1.0*1024*1024,
       'buffer_size' : 50*1024*1024,
       'filter_ratio' : Uniform(0, 0.01), #????? min_ = 0, max_ = 0.01
       
        #reducers
        'no_copiers' : '$::num_of_nodes',
        'reduce_sort_cycles' : 6.0*1000*1000*1000,
        'reduce_cycles_per_byte' : 90.0,
        'reduce_filter_ratio' : Uniform(min_ = 1.0, max_ = 1.0),
    #end hadoop.job.tcl attributes   
    #-------------------------------------------
    #begin hadoop.cluster.tcl
    	'sched' : "[new MRPerf/Scheduler/FairShare]",
    	'put' :	"sched constructed",
    	'heart_beat_delay' : 1,
       	'tasks_to_start' : 2, 	# number of tasks to start at one heartbeat
       	'finish_time' : 5000.0,
        'read_seek' : 0,
        'write_seek' : 0,  
        'timers' : {
                        'alive' : 30,
                        'task_dead' : 600,
                        'ghost_map' : 189,
                        'data_req' : 189
                        },
       	'max_mappers' : 2, 	# per node
       	'max_reducers' : 1, 	# concurrent reducers per node.
    #end hadoop.cluster.tcl attributes   
    #-------------------------------------------
    #begin metadata_gen1.xml
    	'path' : "/data",
    	
    	#number_files
    	'min_files' : 1,
    	'max_files' : 1,
    	
    	#file_size
    	'unit_size' : 67108864,
    	'min_unit' : 1,
    	'max_unit' : 400,
    	
    	'replication_level' : 3,
    	'gen_method' : "random",
    	'name_node' : 'jt',
    	
       	
       	
       	
       	
       	
       	
       	'paddedend' : '1337'
    }
