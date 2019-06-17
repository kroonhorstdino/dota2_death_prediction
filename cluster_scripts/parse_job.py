

import sys
import os
import math
import subprocess
import pickle
import time
from multiprocessing import Pool
import pathlib
from pathlib import PurePath

parent_dir = pathlib.Path.cwd().parent
sys.path.append(str(parent_dir))

import preprocess
import data_loader

# command to delete corrupt files
# grep -r --include="*.log" "PARSING_ERROR: " ./../ |  awk 'NF>1{print $NF}' | awk '$0=$0".h5"' | xargs rm

print(sys.platform)


# Before run:
# generate dem file list with:
# find . -name "*.dem" -type f > /users/ak1774/scratch/esport/death_prediction/all_dem_files.txt
# then set MATCH_FILE_LOCATION_PREFIX
#TODO Run inside script?

MATCH_FILE_LOCATION_PREFIX = parent_dir.parent / 'source_demo_replaydownloader' / 'replays' #TODO set correct path (../replays)
#MATCH_FILE_LOCATION_PREFIX = "/users/ak1774/scratch/cs-dclabs-2019/esport/gamefiles/semipro/"
#MATCH_FILE_LOCATION_PREFIX = "/users/ak1774/scratch/cs-dclabs-2019/esport/gamefiles/pro/"

#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/all_pro_dems.txt"
#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/all_semipro_dems.txt"
#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/some_dem_files.txt"
MATCH_FILE_LIST = parent_dir.parent / 'source_demo_replaydownloader' / 'replays' / 'all_dem_files' #TODO

print(MATCH_FILE_LIST)

JAR_PATH = parent_dir / 'parser' / 'target' / 'my_processor-0.0.one-jar.jar'

ALSO_NORMALIZE = False
NORM_STATS_FILE = parent_dir / 'norm_stats.pickle'


norm_stats = None
if ALSO_NORMALIZE == True:
	with open(NORM_STATS_FILE, 'rb') as f:
		norm_stats = pickle.load(f)

#FOR SLURM
"""
# debug
WORKER_ID = int(os.environ['SLURM_ARRAY_TASK_ID']) 
NUM_WORKERS = int(os.environ['SLURM_ARRAY_TASK_COUNT'])
#WORKER_ID = 1
#NUM_WORKERS = 8

print("I am worker ",WORKER_ID," from ",NUM_WORKERS)
"""

#execution continues at end of file

def parse_files(worker_id):
	#FOR python multiprocessing
	WORKER_ID = worker_id #Current Worker
	NUM_WORKERS = os.cpu_count()

	print("I am worker " + str(WORKER_ID))

	ROOT_DIR = parent_dir
	RESULTS_DIR = parent_dir / 'parsed_files'

	with open(r'Z:\Bachelor\source_demo_replaydownloader\replays\all_dem_files.txt') as f: #MATCH_FILE_LIST
		all_match_files = f.readlines()

	all_match_files = [x.strip() for x in all_match_files] 

	num_matches = len(all_match_files)
	match_per_worker = int(math.ceil(float(num_matches) / NUM_WORKERS))
	print("Match per worker: ",match_per_worker)
	sys.stdout.flush()


	first_match_index_for_this_task = match_per_worker * WORKER_ID
	print("My first match is: ",first_match_index_for_this_task, ". Number of matches is ",num_matches)
	sys.stdout.flush()
	for i in range(match_per_worker):
		match_index = first_match_index_for_this_task + i
		if match_index >= num_matches:
			break # done

    
    
		match_path = all_match_files[match_index]
		match_path = MATCH_FILE_LOCATION_PREFIX / match_path.replace('./','') #Append match name to folder path. Removes ./ before the match name in the all_dem_file

		#match_name = match_path.split("/")[-1].replace(".dem","")
		match_name = match_path.stem

		print("Parsing match number ",match_index," ",match_name)
		sys.stdout.flush()
		#print(match_path)

		subprocess.run(["java","-jar",str(JAR_PATH),str(match_path),str(RESULTS_DIR)])
		print("JAVA finished")
		sys.stdout.flush()

		os.chdir(RESULTS_DIR)

		data = preprocess.read_and_preprocess_data(match_name) #One match
		if data is not None:
			if ALSO_NORMALIZE == True:
				now = time.time()
				data = data_loader.normalize_data(data,norm_stats)
				print("Normalizing took: ", time.time()-now)
				sys.stdout.flush()

			data.to_hdf(match_name + '.h5', key='data', mode='w', complevel = 1,complib='zlib')
			print("H5 created")
		else:
			print("Corrupt match deleted: ",match_name)
		sys.stdout.flush()

		CSV_PAPTH = os.path.join(RESULTS_DIR,match_name + ".csv")
		CSV_LIFE_PAPTH = os.path.join(RESULTS_DIR,match_name + "_life.csv")
		subprocess.run(["rm",CSV_PAPTH])
		subprocess.run(["rm",CSV_LIFE_PAPTH])
		print("CSV deleted")
		sys.stdout.flush()


if __name__ == '__main__': #Split into processes here
	#num_processes = os.cpu_count()
	num_processes = 1

	workers = [i for i in range(num_processes)]
	print("Number of Workers: " + str(len(workers)))

	print("Create multiple processes")
	with Pool(num_processes) as p:
		p.map(parse_files, workers)