#!/bin/bash
set -x

nprocs=8

for i in $(seq 7)
do
./clean.sh
rm -rf /home/storm/Zheming/temp/*

mpiexec -n $nprocs --map-by dist:auto --hostfile hostfile_storm dispel4py mpi /home/storm/Zheming/PageRank.py -d '{"InitialRead" : [{"input" : 1}]}'

while read server
do
scp $server:/home/storm/Zheming/temp/*_*_1 /home/storm/Zheming/temp/
done < hostfile_storm

while read server
do
scp /home/storm/Zheming/temp/*_*_1 $server:/home/storm/Zheming/temp/
done < hostfile_storm

mpiexec -n $nprocs --map-by dist:auto --hostfile hostfile_storm dispel4py mpi /home/storm/Zheming/PageRank.py -d '{"InitialRead" : [{"input" : 2}]}'

while read server
do
scp $server:/home/storm/Zheming/temp/*_*_2 /home/storm/Zheming/temp/
done < hostfile_storm

while read server
do
scp /home/storm/Zheming/temp/*_*_2 $server:/home/storm/Zheming/temp/
done < hostfile_storm

mpiexec -n $nprocs --map-by dist:auto --hostfile hostfile_storm dispel4py mpi /home/storm/Zheming/PageRank.py -d '{"InitialRead" : [{"input" : 3}]}'

done



