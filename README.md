# COMP90024-23S1-A1-Social-Media-Analytics
![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)

## Authors
- Chenyang Dong - doncd@student.unimelb.edu.au
- Un Leng Kam - ukam@student.unimelb.edu.au

## Problem Description
This assignment is to implement a parallelized application leveraging the University of Melbourne HPC facility SPARTAN. The application will use a large Twitter dataset and a file containing the suburbs, locations and Greater Capital cities of Australia. The main objective is to count the number of different tweets made in the Greater Capital cities of Australia, identify the Twitter accounts (users) that have made the most tweets, and identify the users that have tweeted from the most different Greater Capital cities.

## Package Required
- ijson
- mpi4py
- pandas
  
## Resources
The application allows a given number of nodes and cores to be utilized. Specifically, it can be run once to search the bigTwitter.json file on each of the following resources:
- 1 node and 1 core;
- 1 node and 8 cores;
- 2 nodes and 8 cores (with 4 cores per node).

The resources are set when submitting the search application with the appropriate SLURM options.

## Usage
To run the program locally, you can use the following command:
```bash
mpirun -n <number of processes> python <script name> <sal_data> <twitter_data>
```
To run the program on SPARTAN, you can use the following SLURM options:
```bash
sbatch slurm/1n1c.slurm      # For 1 node and 1 core
sbatch slurm/1n8c.slurm      # For 1 node and 8 cores
sbatch slurm/2n8c.slurm      # For 2 nodes and 8 cores (with 4 cores per node)
```
Make sure to contain all the SLURM files in the slurm folder on SPARTAN and have all script files and data files in the root directory. The input file paths in the SLURM files should be updated based on your requirements.


## **Acknowledgments**
- The University of Melbourne
- COMP90024 Cluster and Cloud Computing Subject
- SPARTAN HPC Facility
- Twitter API
