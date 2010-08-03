import json

#modify this to use whatever ns you want
NS = "/h/hydra/home/wanggy/dumbo/ns-allinone-2.33/bin/ns" 

def create_config_file(job):
	config = open("job_config_file", "w")
	config.write(job.config["config_data"])
	config.close()

def create_hadoop_conf(job):
    
    hadoop_conf = open("hadoop_conf.py", 'w')
    hadoop_conf.write('freq_table = ' + json.dumps(job.config['freq_table'], indent=4) + '\n')
    hadoop_conf.write('read_bw_table = ' + json.dumps(job.config['read_bw_table'], indent = 4) + '\n')
    hadoop_conf.write('write_bw_table = ' + json.dumps(job.config['write_bw_table'], indent=4) + '\n')
    hadoop_conf.write('int_bw = \'' + str(job.config['int_bw']) + '\'\n')
    hadoop_conf.write('int_latency = \'' + str(job.config['int_latency']) + '\'\n')
    hadoop_conf.write('ext_bw = \'' + str(job.config['ext_bw']) + '\'\n')
    hadoop_conf.write('ext_latency = \'' + str(job.config['ext_latency']) + '\'\n')
    hadoop_conf.close()
    
def create_hadoop_job_tcl(job):
    hadoop_job = open("hadoop.job.tcl", "w")

#create init_terasort
    hadoop_job.write("proc init_Terasort {job} {\n")
    hadoop_job.write(create_param('jvm_start_cost', job))
    hadoop_job.write(create_param('wrap_up_cost', job))
    hadoop_job.write(create_param('io_factor', job))
    hadoop_job.write(create_param('in_mem_space' ,job))
    hadoop_job.write(create_param('in_mem_segments' ,job))
    hadoop_job.write(create_param('cycles_per_byte', job))
    hadoop_job.write(create_param('sort_cycles_per_byte', job))
    hadoop_job.write(create_param('merge_cycles', job))
    hadoop_job.write(create_param('buffer_size', job))
    hadoop_job.write(create_randomvar('filter_ratio', job.config['filter_ratio'], 1))
    hadoop_job.write(create_param('no_copiers', job))
    hadoop_job.write(create_param('reduce_sort_cycles', job))
    hadoop_job.write(create_param('reduce_cycles_per_byte', job))
    hadoop_job.write(create_randomvar('reduce_filter_ratio', job.config['reduce_filter_ratio'], 1))
    
    

    hadoop_job.write("} \n \n")
    #end init_terasort
        
    #create init_search
    hadoop_job.write(
        """proc init_Search {job cycles_per_byte} {
        init_Terasort $job
        $job set cycles_per_byte $cycles_per_byte
        [$job set filter_ratio] set min_ 0.0
        [$job set filter_ratio] set max_ 0.0001
}

""")
    #create init_Index
    hadoop_job.write(
       """proc init_Index {job filter_ratio} {
   init_Terasort $job
   [$job set filter_ratio] set min_ $filter_ratio
   [$job set filter_ratio] set max_ $filter_ratio
}""")
    hadoop_job.close()


def create_metadata_gen1(job):
    metadata = open("metadata_gen1.xml", "w")
    metadata.write("""<?xml version="1.0" encoding="UTF-8"?>
<conf xsi:noNamespaceSchemaLocation="metadata_gen.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
""")
    metadata.write(create_xmlparam('path', job, 1))
        
    metadata.write("\t<number_files>\n")
    metadata.write(create_xmlparam('min_files', job, 2))
    metadata.write(create_xmlparam('max_files', job, 2))
    metadata.write("\t</number_files>\n")
        
    metadata.write("\t<file_size>\n")
    metadata.write(create_xmlparam('unit_size', job, 2))
    metadata.write(create_xmlparam('min_unit', job, 2))
    metadata.write(create_xmlparam('max_unit', job, 2))
    metadata.write("\t</file_size>\n")
        
        
    metadata.write(create_xmlparam('replication_level', job, 1))
    metadata.write(create_xmlparam('gen_method', job, 1))
    metadata.write(create_xmlparam('name_node', job, 1))
        
    metadata.write("</conf>\n")
    metadata.close()
        
        
def create_hadoop_cluster(job):
    hadoop_cluster = open('hadoop.cluster.tcl', 'w')
        
    hadoop_cluster.write(create_param2("sched", job))
    hadoop_cluster.write("puts \"" + job.config['put'] + '\"\n')
    hadoop_cluster.write(create_param2("tasks_to_start", job) )
    hadoop_cluster.write(create_param2("finish_time", job) )
    hadoop_cluster.write(create_param2("read_seek", job) )
    hadoop_cluster.write(create_param2("write_seek", job) )
        
    #timers
    hadoop_cluster.write("set timers(alive) " + str(job.config['timers']['alive']) + '\n')
    hadoop_cluster.write("set timers(task_dead) " + str(job.config['timers']['task_dead']) + '\n')
    hadoop_cluster.write("set timers(ghost_map) " + str(job.config['timers']['ghost_map']) + '\n')
    hadoop_cluster.write("set timers(data_req) " + str(job.config['timers']['data_req']) + '\n')
        
    hadoop_cluster.write(create_param2("heart_beat_delay", job))
    hadoop_cluster.write("$sched max_mappers " + str(job.config['max_mappers']) + '\n')
    hadoop_cluster.write(create_param2("max_reducers", job))
#helper functions

def create_script(job):
    script = open(job.scriptfile, 'w')
    script.write("#!/bin/bash\n" + \
"cd " + job.resultdir + \
"\nmake topo > " + job.makelog + \
"\nmake gen >> " + job.makelog + \
"\n(time " + NS + " hsim.tcl) &> " + job.nslog + "NS")

def create_param(param, job):
    return "\t$job set " + param + ' ' + str(job.config[param]) + '\n'

def create_param2(param, job):
    return "set " + param + ' ' + str(job.config[param]) + '\n'


def create_randomvar(name, var, numtabs):
    if var['type'] == 'Uniform':
        return(
    "\t"*numtabs + "$job set " + name + " [new RandomVariable/Uniform]\n" +\
    "\t"*numtabs + "[$job set " + name + "] set min_ " + str(var['min']) + '\n' +\
    "\t"*numtabs + "[$job set " + name + "] set max_ " + str(var['max']) + '\n')
    
def create_xmlparam(param, job, numtabs):
    return "\t"*numtabs + "<" + param + ">" + str(job.config[param]) + "</" + param + ">\n"
 
