#!/bin/bash -l
#SBATCH --job-name=AERA_T_test01
#SBATCH --account=nn2980k
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=02:00:00
#SBATCH --output=AERA_T_test01_%j.out
#SBATCH --error=AERA_T_test01_%j.err
#SBATCH --export=ALL

set -euo pipefail

echo "======================================"
echo "Job started at: $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node list: ${SLURM_NODELIST}"
echo "Working directory: $(pwd)"
echo "======================================"

# Conda environment
source ~/.bash_conda
conda activate kdask

echo "Python executable:"
which python
python --version

# Move to AERA working directory
cd /cluster/projects/nn2980k/yongyub/NORESM/NorESM2/run_script/AERA/AERA_T

echo "Running load script..."
python -u load_NSSP126_AERA_T_test01.py > log_load 2>&1

echo "Running scale script..."
python -u scale_NSSP126_AERA_T_test01.py > log_scale 2>&1

echo "======================================"
echo "Job finished at: $(date)"
echo "======================================"
