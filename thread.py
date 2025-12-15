import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor

# Function to read and write chunks
def process_chunk(chunk, chunk_number):
    print("fun",chunk_number)
    output_file = f"chunk_{chunk_number}.csv"
    chunk.to_csv(output_file, index=False)
    print(f"Chunk {chunk_number} written to {output_file}")

# Function to read large CSV in chunks and process in parallel
def process_large_csv(input_file, chunk_size):
    chunk_number = 1
    print(chunk_number)
    with ThreadPoolExecutor(max_workers=10) as executor:
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            executor.submit(process_chunk, chunk, chunk_number)
            chunk_number += 1

if __name__ == "__main__":
    input_file = r"C:\Users\manga\Downloads\qwe.csv"
    chunk_size = 10000000  # Adjust based on your memory constraints
    process_large_csv(input_file, chunk_size)
