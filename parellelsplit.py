# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 08:25:21 2024

@author: manga
"""

import os
import pandas as pd
from multiprocessing import Pool,cpu_count

print("DINESH")


chunksize=10**7
filename=r"C:\Users\manga\Downloads\xyz.csv"


for i,chunk in enumerate(pd.read_csv(filename,chunksize=chunksize)):
    print(i)
    chunk.to_csv(f'chunk_{i}.csv',index=False)
    



