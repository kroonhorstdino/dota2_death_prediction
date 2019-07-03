import sys
from pathlib import Path
from multiprocessing import Pool

sys.path.append(str(Path.cwd()))

import data_loader

'''
if __name__ == '__main__':  # Split into processes here

	worker_count = utility.get_worker_count()

	arguments = [(None,i,worker_count) for i in range(worker_count)] #Arguments for each function call in the pool
	print("Number of Workers: " + str(len(arguments)))

	print("Create multiple processes")
	with Pool(worker_count) as p:
		p.starmap(data_loader.run_cluster_calculate_norm_stats(),
		      arguments)  # Work in parallel
'''

data_loader.run_cluster_normalize(None, 0)
