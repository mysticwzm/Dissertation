#!/bin/sh
#Generate the initial array
python ArrayGenerator.py
#10 iterations of workflow
nprocs=8
# START=`date +%s%N`;
# simple
# dispel4py simple IS.py -d '{"Init" : [{"input" : 1}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 2}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 3}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 4}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 5}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 6}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 7}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 8}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 9}]}'
# dispel4py simple IS.py -d '{"Init" : [{"input" : 10}]}'
# mpi
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 1}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 2}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 3}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 4}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 5}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 6}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 7}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 8}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 9}]}'
# mpiexec -n $nprocs dispel4py mpi IS.py -d '{"Init" : [{"input" : 10}]}'
# multi
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 1}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 2}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 3}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 4}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 5}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 6}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 7}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 8}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 9}]}'
dispel4py multi IS.py -n $nprocs -d '{"Init" : [{"input" : 10}]}'
# END=`date +%s%N`;
# time=$((END-START))
# time=`expr $time / 1000000`
# echo "total time : "$time

#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/exports/applications/apps/SL6/MPI/openmpi/gcc/1.4.5-gcc_4.4.6/lib/