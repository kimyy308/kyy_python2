#!/bin/sh


#mpirun --np 6 dask-mpi --worker-class distributed.Worker --scheduler-file /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/scheduler.json --dashboard-address :2822 --memory-limit=90e9 --local-directory /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/tmp/
mpirun --np 6 dask-mpi --worker-class distributed.Worker --scheduler-file /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/scheduler.json --dashboard-address :2822 --memory-limit=29e9 --local-directory /nird/home/yongyub/kimyy/Dropbox/source/python/all/mpi_nird/tmp/


