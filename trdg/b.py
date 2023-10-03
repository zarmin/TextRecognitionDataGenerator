#!/usr/bin/env python3

import multiprocessing
import tqdm
import functools

class FakeTextDataGenerator(object):
  @classmethod
  def abc(cls, index, text, stuff, stuff2, stuff3):
    # print(cls, index, text, stuff, stuff2, stuff3)
    x = 0
    for j in range(10000):    
      x += j
    return x

  @classmethod
  def generate_from_tuple(cls, t):
    # print(a)
    cls.abc(*t)

def main():
  thread_count = 18

  manager = multiprocessing.Manager()
  shared_queue = manager.Queue(maxsize = 4096)
  p = multiprocessing.Pool(processes=thread_count)
  # partial_worker = functools.partial(FakeTextDataGenerator.generate_from_tuple, shared_queue)

  strings = []
  for i in range(10240):
    strings.append(str(i))

  string_count = len(strings)

  cnt = string_count

  pbar = tqdm.tqdm(
    p.imap_unordered(
      FakeTextDataGenerator.generate_from_tuple,
        zip(
          [i for i in range(0, string_count)],
          strings,
          ["abc"] * string_count,
          [12345] * string_count,
          [shared_queue] * string_count,
        ),
      ),
      total=cnt
  )

  print("Wut1")

  list(pbar)

  print("Wut2")

  pbar.close()
  p.terminate()  

  print("Wut3")

if __name__ == "__main__":
  main()
