#!/bin/sh

#mpirun -np 6 dask-mpi --scheduler-file /proj/sahils/mpi/scheduler.json --dashboard-address :8341 --memory-limit=90e9 --worker-class distributed.Worker

mpirun --np 6 dask-mpi --worker-class distributed.Worker --scheduler-file /proj/kimyy/Dropbox/source/python/all/mpi/scheduler.json --dashboard-address :2822 --memory-limit=90e9 --local-directory /proj/kimyy/Dropbox/source/python/mpi/tmp/

#mpirun --np 6 dask-mpi --scheduler-file /proj/sahils/mpi/scheduler.json --no-nanny --dashboard-address :4142 --memory-limit=90e9  --local-directory /proj/sahils/mpi/tmp/
