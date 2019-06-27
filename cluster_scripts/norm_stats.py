import sys
from pathlib import Path

sys.path.append(str(Path.cwd().parent))

import data_loader

data_loader.run_cluster_calculate_norm_stats()