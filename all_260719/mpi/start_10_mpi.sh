#!/bin/sh

mpirun --np 11 dask-mpi --worker-class distributed.Worker --scheduler-file /proj/kimyy/Dropbox/source/python/all/mpi/scheduler_10.json --dashboard-address :2223 --memory-limit=50e9 --local-directory /proj/kimyy/Dropbox/source/python/mpi/tmp_10/


