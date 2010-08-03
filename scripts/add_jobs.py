#usage: python add_job.py config_file

#Adds jobs represented in a config into the queue used by the scheduler, merging any default values located in job.py

#Config files should be json map objects representing key, value pairs in job.py
#ie. If you want to overwrite the default values in job.py you would
#write something like this to represent two different jobs:
"""


{
    "description" : "Job 1",
    "in_mem_space": 500.0, 
    "sort_cycles_per_byte": 500.0
};;;

{
    "description" : "Job 2",
    "in_mem_space": 300.0, 
    "sort_cycles_per_byte": 200.0
}


"""

#Notice that there is no ";;;" on the last object in the file
#To run jobs you would then use the scheduler by typing: 
#python scheduler.py

import sys
import json
from job import JobTask


if __name__ == "__main__":
    try:
        inputfile = open(sys.argv[1])
    except IOError:
        print("File does not exist.\n")
        sys.exit(2)
    except IndexError:
            print("No argument was given. Exiting...")
            sys.exit(2)
    string = inputfile.read()
    inputfile.close()
    jobtasks = string.split(';;;')
    queue = open('queue', 'a')
    
    for x in jobtasks:
        job = JobTask()
        temp = json.loads(x)
        job.config.update(temp)
        job.config["config_data"] = x
        queue.write(json.dumps(job.config, indent=4))
        queue.write(';;;\n\n\n');
    
    queue.close()             
    
    print("Add_Jobs Done")
