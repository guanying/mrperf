import os
import subprocess 
from filecreator import *
import io
import threading


BASEDIR = os.path.split(os.getcwd())[0]
print("BASEDIR is: " + BASEDIR)
SIMDIR = os.path.join(BASEDIR, "sim")
SRC = os.path.join(BASEDIR,"src")
TEMPDIR = os.path.join(BASEDIR, "temp")
NEXTFILE = os.path.join(BASEDIR, "sim/nextID")
CONFDIR = os.path.join(BASEDIR, "conf")

def setup_job(Job, ID):
    
    TOPOLOGYFILE = os.path.join(CONFDIR, Job.config["topology_file"])
    RESULTDIR = os.path.join(SIMDIR, "sim-" + str(ID))
    Job.resultdir = RESULTDIR
    subprocess.call(["cp", "-a", SRC, RESULTDIR])
    subprocess.call(["cp", TOPOLOGYFILE, os.path.join(RESULTDIR, "topology1.xml")])
    os.chdir(RESULTDIR)

    Job.id = ID
    Job.resultdir = RESULTDIR
    Job.makelog = os.path.join(RESULTDIR, "makelog")
    Job.nslog = os.path.join(RESULTDIR, "nslog")
    Job.scriptfile = os.path.join(RESULTDIR, "run_exp.sh")
    create_hadoop_conf(Job)
    create_hadoop_job_tcl(Job)
    create_metadata_gen1(Job)
    create_hadoop_cluster(Job)
    create_config_file(Job)
    create_script(Job)
    subprocess.call(["chmod", "+x", Job.scriptfile])
    os.chdir(BASEDIR)
        

def nextID():
    idfile = open(NEXTFILE,'r')
    id = int(idfile.read())
    idfile.close()
    idfile = open(NEXTFILE, 'w')
    idfile.write(str(id+1))
    idfile.close()
    return id

class JobThread(threading.Thread):
    def __init__ (self, job, id, username, node):
            self.username = username
            self.job = job
            self.id = id
            self.node = node
            threading.Thread.__init__ (self)
            
            
    def run(self):
            a = subprocess.Popen(args="ssh " + self.username + "@" + self.node + " " + self.job.scriptfile, shell=1)
            self.exit_code = a.wait()
            print('id: ' + str(self.id) + " make output is located at: " + self.job.makelog + \
             '\nid: ' + str(self.id) + " NS output is located at: " + self.job.nslog + \
            '\nid: ' + str(self.id) + " is done. Exited with exit code: " + str(self.exit_code))

