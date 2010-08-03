#!/bin/bash

#cp $1/job_*_data.xml .
#cp $1/job.trace.tcl .

N=`cat job.trace.tcl | wc -l`

head -n $(($1-1)) job.trace.tcl > temp
tail -n $(($N-$1)) job.trace.tcl > temp2
cat temp temp2 > job.trace.tcl
rm temp temp2

for i in `seq $(($1+1)) $N`
do
	old=`printf "job_%04d_data.xml" $(($i))`
	new=`printf "job_%04d_data.xml" $(($i-1))`
	mv $old $new
	#j=`printf "%04d" $(($i+1))`
	#mv job_`printf "%04d" $(($i+1))`\_data.xml job_$i\_data.xml
done

touch $1.removed.from.$N

