#This program reads in json objects representing job tasks, located in the file "queue"
#It then creates a thread for each job, running the thread on the first available node
#Each job can run on different nodes by writing the value of "nodes" inthe config file for that job
#This program waits for all threads to be finished before exiting.




#this value represents how many jobs can run on any node at a given time
MAXJOBS = 2

#this value represents the user name you use for ssh, make sure you have any ssh keys setup between your nodes
username = "cbrahms"

import json #for encoding
import time #for sleep
import sys #for exiting
from exp import *
from job import *
class NodeMap:
		#used to keep track of nodes and active threads in those nodes
	def __init__(self):
		self.nodes = {}
	def incr(self, node):
		try:
			self.nodes[node] += 1
		except KeyError:
			self.nodes[node] = 1
	def decr(self, node):
		try:
				self.nodes[node] -= 1
		except KeyError:
				self.nodes[node] = 0
	def get(self, node):
		try:
			to_return =  self.nodes[node]
			return to_return
		except KeyError:
			self.nodes[node] = 0
			return 0

if __name__ == "__main__":        
		nodeMap = NodeMap()
		queuefile = open('queue','r')
		jobtasks = queuefile.read().split(';;;')
		queuefile.close()
		queuefile = open('queue', 'w') #clear the queue file
		queuefile.close()
		jobtasks.pop() #remove empty value at the end
		job_queue = []
		thread_queue = []
		for x in jobtasks:
			newtask = JobTask()
			newtask.config = json.loads(str(x))
			job_queue.append(newtask) 
				
		for job in job_queue:
			setup_job(job, nextID())
		print("Total number of of jobs to run is: " + str(len(job_queue)))
		if len(job_queue) == 0:
				sys.exit(-1)
		notFinished = True
		while notFinished:
			for job in job_queue:
				for thread in thread_queue:
						if not thread.isAlive():
								nodeMap.decr(thread.node)
								
				#check every node this job can run at to see if that node is available				
				for node in job.config["nodes"]:
					if nodeMap.get(node) < MAXJOBS: #if less than two jobs are active on this node
						#use this node
							print("Adding job id: " +str(job.id) + " to thread queue, and starting on node: " + node)
							thread = JobThread(job, job.id, username, node)
							thread_queue.append(thread)
							thread.start()
							nodeMap.incr(node)
							if len(thread_queue) == len(job_queue):
									#we now have all jobs in threads
									print("All threads have been started...")
									notFinished = False
							break #exit the for loop
			
			time.sleep(30) #sleep 30 seconds between checks
			
		#check if all threads are done	
		notFinished = True
		while notFinished:
			found = False
			for thread in thread_queue:
				if thread.isAlive():
					found = True
			if not found:
				notFinished = False
			else:
				time.sleep(30) #sleep 30 seconds between checks
				
		print("All threads are done.")		
			



