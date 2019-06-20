#CUSTOM FILE FOR UTITLITY FUNCTIONS
import os

#How many workers
def get_worker_count():
	return os.cpu_count()
	#return 1
