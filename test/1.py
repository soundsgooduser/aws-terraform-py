import _thread
import time

# Define a function for the thread
def process_key(threadName, delay):
  count = 0
  count += 1
  print(threadName)


try:
  for index in range(1,10):
    _thread.start_new_thread(process_key, ("Thread-" + str(index), 2))
except:
  print ("Error: unable to start thread")