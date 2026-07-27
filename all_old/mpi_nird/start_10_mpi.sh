#!/bin/sh

mpirun --np 11 dask-mpi --worker-class distributed.Worker --scheduler-file /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/scheduler_10.json --dashboard-address :2223 --memory-limit=50e9 --local-directory /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/tmp_10/


