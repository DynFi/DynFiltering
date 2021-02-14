import threading as th
import DatabaseWriter as DW
import time


def main():
    """
    Create 100 threads on DatabaseWriter for optimized parsing
    """
    number_threads = 100   #Max number of threads is 100 because of postgresql limit (maybe change it to optimize speed))
    threads = []
    for x in range(number_threads):
        thread = th.Thread(target=DW.main,args=(x, number_threads))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()

