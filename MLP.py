# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 09:14:59 2024

@author: manga
"""

print("dinesh")


import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


# Function to read and write chunks
def process_chunk(chunk, chunk_number):
    print("Test3",chunk_number)
    output_file = f"chunk_{chunk_number}.csv"
    chunk.to_csv(output_file, index=False)
    print(f"Chunk {chunk_number} written to {output_file}")

# Function to read large CSV in chunks and process in parallel
def process_large_csv(input_file, chunk_size):
    chunk_number = 1
    print(chunk_number)
    with ProcessPoolExecutor() as executor:
        print("Entered")
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            print("Test1",chunk_number)
            executor.submit(process_chunk, chunk, chunk_number)
            print("Test2",chunk_number)
            chunk_number += 1

if __name__ == "__main__":
    input_file = r"C:\Users\manga\Downloads\qwe.csv"
    chunk_size = 10000000  # Adjust based on your memory constraints
    process_large_csv(input_file, chunk_size)
