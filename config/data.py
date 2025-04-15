from pyspark.sql.types import *
import os
from pathlib import Path

parent_path= Path().resolve().parent

def get_path(file_path):
    return os.path.join(parent_path, file_path)

data= {
    "raw": get_path("data/00-raw-data"),
    "raw-extracted":{
        "billing-reports": get_path("data/01-raw-extracted-data/01-billing-reports"),
        "project-allocations": get_path("data/01-raw-extracted-data/02-project-allocations")        
    },
    "intermediate":{
        "billing-reports": get_path("data/02-intermediate-data/01-billing-reports"),
        "billing-analysis": get_path("data/02-intermediate-data/02-billing-analysis"),
        "project-allocations": get_path("data/02-intermediate-data/03-project-allocations")
    },
    'checkpoint':{
        "billing-reports": get_path("data/03-checkpoint-data/01-billing-reports"),
        "project-allocations": get_path("data/03-checkpoint-data/02-project-allocations")
    },
    "processed":  get_path("data/04-processed-data")
}

