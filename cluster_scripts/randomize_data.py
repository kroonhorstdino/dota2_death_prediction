import sys
from pathlib import Path
from multiprocessing import Pool

sys.path.append(str(Path.cwd()))

import data_loader
import utility

if __name__ == '__main__':  # Split into processes here

    data_type = sys.argv[1]

    if((data_type in ["train","validation","test"]) != True):
        sys.exit('Enter either train, validate or test!')

    worker_count = utility.get_worker_count()

    arguments = [(data_type,i,worker_count) for i in range(worker_count)]
    print(arguments)
    print("Number of Workers: " + str(len(arguments)))

    #print("Create multiple processes")
    #with Pool(worker_count) as p:
    #    p.starmap(data_loader.run_cluster_randomize, arguments)  # Work in parallel
    for p in arguments:
         data_loader.run_cluster_randomize(data_type,p[1],worker_count)

