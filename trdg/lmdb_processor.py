import queue
import time
import lmdb

def put_into_lmdb(txn, message):
    name, text, png = message
    txn.put(('image-' + name).encode(), png)
    txn.put(('label-' + name).encode(), text)

def update_num_samples_and_commit(txn, i):
    txn.put('num-samples'.encode(), str(i - 1).encode())
    txn.commit()

def lmdb_processor(shared_queue, lmdb_dir):
    CHUNK_SIZE = 4096
    env = lmdb.open(lmdb_dir, map_size=int(1e12))

    i = 0
    txn = env.begin(write=True)

    while True:
        i += 1        
        try:
            message = shared_queue.get()            
            if message is None:
                update_num_samples_and_commit(txn, i)
                break

            put_into_lmdb(txn, message)

            if i % CHUNK_SIZE == 0:
                update_num_samples_and_commit(txn, i)
                txn = env.begin(write=True)

        except queue.Empty:
            time.sleep(0.1)            
