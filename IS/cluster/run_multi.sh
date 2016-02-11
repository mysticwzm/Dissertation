#!/bin/sh
#$ -N IS_B_32
#$ -cwd
#$ -pe OpenMP 8
#$ -R y
#$ -l h_rt=02:00:00

# Initialise the modules environment
. /etc/profile.d/modules.sh

# Use Anaconda module
module load anaconda/2.1.0
#module load intel

# 10 iterations of workflow
# multi
nprocs=32
# 5 tests
for i in $(seq 7)
do
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 1}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 2}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 3}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 4}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 5}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 6}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 7}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 8}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 9}]}'
	python -m dispel4py.new.processor multi Zheming/IS.py -n $nprocs -d '{"Init" : [{"input" : 10}]}'
done

