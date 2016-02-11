#!/bin/sh
#3 iterations of workflow
nprocs=8
# START=`date +%s%N`;
# simple
# dispel4py simple PageRank.py -d '{"InitialRead" : [{"input" : 1}]}'
# dispel4py simple PageRank.py -d '{"InitialRead" : [{"input" : 2}]}'
# dispel4py simple PageRank.py -d '{"InitialRead" : [{"input" : 3}]}'
# mpi
# mpiexec -n $nprocs dispel4py mpi PageRank.py -d '{"InitialRead" : [{"input" : 1}]}'
# mpiexec -n $nprocs dispel4py mpi PageRank.py -d '{"InitialRead" : [{"input" : 2}]}'
# mpiexec -n $nprocs dispel4py mpi PageRank.py -d '{"InitialRead" : [{"input" : 3}]}'
# multi
dispel4py multi PageRank.py -n $nprocs -d '{"InitialRead" : [{"input" : 1}]}'
dispel4py multi PageRank.py -n $nprocs -d '{"InitialRead" : [{"input" : 2}]}'
dispel4py multi PageRank.py -n $nprocs -d '{"InitialRead" : [{"input" : 3}]}'
# END=`date +%s%N`;
# time=$((END-START))
# time=`expr $time / 1000000`
# echo "total time : "$time