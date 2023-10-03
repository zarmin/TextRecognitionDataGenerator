import queue
import time
import lmdb
import math
import copy

def put_into_queue(txn, message):
    name, text, png = message
    txn.put(('image-' + name).encode(), png)
    txn.put(('label-' + name).encode(), text)

def update_num_samples_and_commit(txn, i):
    txn.put('num-samples'.encode(), str(i - 1).encode())
    txn.commit()

def lmdb_processor(shared_queue, lmdb_dir, count):
    default_map_size_per_entry = 4 * 1024

    map_size = count * default_map_size_per_entry
    CHUNK_SIZE = 4096
    env = lmdb.open(lmdb_dir, map_size=map_size, writemap=True)

    i = 0
    txn = env.begin(write=True)
    retry_queue = queue.Queue()
    dirty = False

    while True:
        i += 1        
        try:
            if dirty:                
                while not retry_queue.empty():
                    put_into_queue(txn, retry_queue.get())
                retry_queue = queue.Queue()
                dirty = False
                update_num_samples_and_commit(txn, i)
                txn = env.begin(write=True)

            message = shared_queue.get()            
            if message is None:
                update_num_samples_and_commit(txn, i)
                break

            retry_queue.put(copy.deepcopy(message))
            put_into_queue(txn, message)

            if i % CHUNK_SIZE == 0:
                update_num_samples_and_commit(txn, i)
                retry_queue = queue.Queue()
                txn = env.begin(write=True)
        except queue.Empty:
            time.sleep(0.1)
        except lmdb.MapFullError:
            map_size = math.ceil(map_size * 1.333)
            print("Increasing LMDB map size to", map_size)
            env.close()
            
            env = lmdb.open(lmdb_dir, map_size=map_size, writemap=True)

            txn = env.begin(write=True)
            dirty = True            
