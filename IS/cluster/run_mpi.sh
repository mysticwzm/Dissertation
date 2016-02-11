#!/bin/sh
#$ -N IS_B_32
#$ -cwd
#$ -pe openmpi_fillup_mark2 4
#$ -R y
#$ -l h_rt=02:00:00


# Initialise the modules environment
. /etc/profile.d/modules.sh

# Use Anaconda module
module load anaconda/2.1.0
# module load python/2.7.5
# module load openmpi-gcc

# 10 iterations of workflow
# mpi
nprocs=32

# 5 tests
for i in $(seq 7)
do
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 1}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 2}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 3}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 4}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 5}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 6}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 7}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 8}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 9}]}'
	mpiexec -n $nprocs python -m dispel4py.new.processor mpi Zheming/IS.py -d '{"Init" : [{"input" : 10}]}'
done
