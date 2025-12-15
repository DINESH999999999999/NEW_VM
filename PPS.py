
import pandas as pd
import os
import pandas as pd
from multiprocessing import Pool, cpu_count

#chunksize = 10**5  # Adjust this based on your file's data density
chunksize = 10**7
filename=r"C:\Users\manga\Downloads\qwe.csv"

'''

for i, chunk in enumerate(pd.read_csv(filename, chunksize=chunksize)):
    chunk.to_csv(f'chunk_{i}.csv', index=False)
'''

def func(chunkz,ii):
    print(ii)
    chunkz.to_csv(f'chunk_{ii}.csv', index=False)



print(os.getcwd())

for i, chunk in enumerate(pd.read_csv(filename, chunksize=chunksize)):
    print(i)
    with Pool(cpu_count()) as pool:
        pool.map(func,chunk,i)
    #chunk.to_csv(f'chunk_{i}.csv', index=False)

print(os.getcwd())