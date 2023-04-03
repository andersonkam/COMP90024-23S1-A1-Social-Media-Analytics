#!/bin/bash

# Partition for the job:
#SBATCH --partition=physical

# The name of the job:
#SBATCH --job-name="1N1C"

# The project ID which this job should run under:
#SBATCH --account="COMP90024"

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

# Use this email address:
#SBATCH --mail-user=doncd@student.unimelb.edu.au

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# ends successfully
#SBATCH --mail-type=END

#SBATCH --output=output/1n1c.txt

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-1:0:00

# Run the job from your home directory:
cd $HOME

module load gcccore/10.2.0 python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
virtualenv ~/venvs/venv-3.7.4
source ~/venvs/venv-3.7.4/bin/activate
# pip install pandas
# pip install ijson

time mpirun -n 1 python main.py sal.json bigTwitter.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s