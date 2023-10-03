import multiprocessing
import threading
import queue
import functools

def worker(shared_queue, task):
    """
    Worker function to compute the sum of elements in the range and 
    put the result in the shared queue.
    """
    task_id, data_range = task
    data_sum = sum(data_range)
    shared_queue.put((task_id, data_range, data_sum))

def queue_processor(shared_queue):
    """
    Thread function to process messages from the shared queue.
    """
    while True:
        try:
            # Get a message from the queue and process it.
            message = shared_queue.get()
            if message == "STOP":
                print("Queue Processor received STOP signal!")
                break
            task_id, data_range, data_sum = message
            print(f"Processed Task {task_id}: Sum of {list(data_range)} is {data_sum}")
        except queue.Empty:
            continue

if __name__ == "__main__":
    # Number of worker processes to spawn.
    num_workers = 4
    
    # Create a manager object to manage shared data.
    with multiprocessing.Manager() as manager:
        # Create a shared queue to be used by all processes.
        shared_queue = manager.Queue()

        # Create some sample tasks.
        tasks = [(i, range(i, i + 5)) for i in range(100)]

        # Start the queue processor thread.
        processor_thread = threading.Thread(target=queue_processor, args=(shared_queue,))
        processor_thread.start()

        # Create a process pool and fill the shared queue using imap_unordered.
        with multiprocessing.Pool(processes=num_workers) as pool:
            partial_worker = functools.partial(worker, shared_queue)
            list(pool.imap_unordered(partial_worker, tasks))
        
        # Send STOP signal to the queue processor thread.
        shared_queue.put("STOP")
        processor_thread.join()

    print("All tasks completed.")

