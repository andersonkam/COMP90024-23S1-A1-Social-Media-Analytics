#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=1n1c.txt
#SBATCH --mail-user=doncd@student.unimelb.edu.au
#SBATCH --mail-type=End
#SBATCH --time=0-12:00:00

my_home="/home/$USER"

source ~/venvs/venv-3.7.4/bin/activate
module load python/3.7.4
module load ijson
module load mpi4py/3.0.2-timed-pingpong 

time mpirun -n 1 python main.py sal.json bigTwitter.json