print("GOVINDA")
import datetime
import subprocess
'''
curr_datetime = str(datetime.datetime.now())[:16]
curr_datetime=curr_datetime.replace(" ","_")
curr_datetime=curr_datetime.replace(":","")
curr_datetime=curr_datetime.replace("-","")
print(curr_datetime)

cur=datetime.datetime.now()
print(cur)
'''

'''
cmd = ["tbuild", "-f", "/media/ssd/tptscripts/DEMO_USER_BNM_TPT_JOB.tpt", "-C"]


t=subprocess.run(cmd, capture_output=True, text=True)

print(t)
print(t.returncode)
print(t.stdout)
'''

import concurrent.futures
import time

# Sample function that each thread will execute
def task(n):
    print(f"Thread {n} started.")
    time.sleep(2)  # Simulate work with a sleep
    print(f"Thread {n} finished.")
    return f"Result from thread {n}"

# Using ThreadPoolExecutor
def main():
    # Create a ThreadPoolExecutor with 3 workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the executor
        futures = {executor.submit(task, i): i for i in range(15)}  # Submitting 5 tasks
        
        # Wait for the results and capture the return code (result) of each thread
        for future in concurrent.futures.as_completed(futures):
            thread_num = futures[future]
            try:
                result = future.result()  # This blocks until the thread is done
                print(f"Thread {thread_num} returned: {result}")
            except Exception as e:
                print(f"Thread {thread_num} generated an exception: {e}")

if __name__ == "__main__":
    main()
    
