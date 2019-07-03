

import sys
import os
import math
import subprocess
import pickle
import time
import multiprocessing as mp
from multiprocessing import Pool
import pathlib
from pathlib import Path

parent_dir = Path.cwd().parent
sys.path.append(str(parent_dir))

import preprocess
import data_loader
import utility

# command to delete corrupt files
# grep -r --include="*.log" "PARSING_ERROR: " ./../ |  awk 'NF>1{print $NF}' | awk '$0=$0".h5"' | xargs rm

# Before run:
# generate dem file list with:
# find . -name "*.dem" -type f > all_dem_files.txt
# then set MATCH_FILE_LOCATION_PREFIX

MATCH_FILE_LOCATION_PREFIX = parent_dir.parent / 'source_demo_replaydownloader' / 'replays'
#MATCH_FILE_LOCATION_PREFIX = "/users/ak1774/scratch/cs-dclabs-2019/esport/gamefiles/semipro/"
#MATCH_FILE_LOCATION_PREFIX = "/users/ak1774/scratch/cs-dclabs-2019/esport/gamefiles/pro/"

#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/all_pro_dems.txt"
#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/all_semipro_dems.txt"
#MATCH_FILE_LIST = "/users/ak1774/scratch/esport/death_prediction/some_dem_files.txt"
MATCH_FILE_LIST = parent_dir.parent / 'source_demo_replaydownloader' / 'replays' / 'all_dem_files.txt' #TODO

JAR_PATH = parent_dir / 'parser' / 'target' / 'my_processor-0.0.one-jar.jar'

ALSO_NORMALIZE = True
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

def parse_files(worker_id):

	#FOR python multiprocessing
	WORKER_ID = worker_id #Current Worker
	NUM_WORKERS = utility.get_worker_count()

	print("I am worker " + str(WORKER_ID))

	ROOT_DIR = parent_dir
	RESULTS_DIR = parent_dir / 'parsed_files'

	with open(str(MATCH_FILE_LIST)) as f: #MATCH_FILE_LIST
		all_match_files = f.readlines()

	all_match_files = [x.strip() for x in all_match_files] 

	num_matches = len(all_match_files)
	match_per_worker = int(math.ceil(float(num_matches) / NUM_WORKERS))
	print("Match per worker: ", match_per_worker)
	sys.stdout.flush()


	first_match_index_for_this_task = match_per_worker * WORKER_ID
	print("My first match is: ",first_match_index_for_this_task, ". Number of matches is ",num_matches)
	sys.stdout.flush()

	time.sleep(1)

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
		
		#CSV_PATH = 'G:/Bachelor/dota2_death_prediction/parsed_files/4844929615_849875286.csv'
		#CSV_LIFE_PATH = 'G:/Bachelor/dota2_death_prediction/parsed_files/4844929615_849875286_life.csv'

		CSV_PATH = RESULTS_DIR / (match_name + '.csv')
		CSV_LIFE_PATH = RESULTS_DIR / (match_name + '_life.csv')

		#ubprocess.run(["rm",str(CSV_PATH)])
		#subprocess.run(["rm",str(CSV_LIFE_PATH)])
		os.remove(CSV_PATH)
		os.remove(CSV_LIFE_PATH)

		print("CSV deleted")
		sys.stdout.flush()


if __name__ == '__main__': #Split into processes here

	worker_count = utility.get_worker_count()

	workers = [i for i in range(worker_count)]
	print("Number of Workers: " + str(len(workers)))
	
	print("Create multiple processes")
	with Pool(worker_count) as p:
		p.map(parse_files, workers)  # Work in parallel
	
	#parse_files(0) #For debugging
