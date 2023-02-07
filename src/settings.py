import os
import sys
import warnings

import pandas as pd
import pyspark.pandas as ps

# Combine series from different dataframes in pyspark

ps.set_option("compute.ops_on_diff_frames", True)

# Remove certain warnings (should be phased out)
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Make the nst module accessible from notebook
sys.path.append("/home/jovyan/tech-coach/src")

# OS settings
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# Pandas settings
pd.options.mode.chained_assignment = None
pd.options.display.max_rows = 20
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 150)
