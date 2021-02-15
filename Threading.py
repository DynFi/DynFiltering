import threading as th
import DatabaseWriter as DW
import time


def main(conn_db):
    """
    Create 100 threads on DatabaseWriter for optimized parsing
    """
    number_threads = 200   #Max number of threads is 100 because of postgresql limit
    # Best is to increase the nbr of threads because it could go faster 
    # with a nbr of threads between 100 and 1000 (change the postgresql limit first)
    threads = []
    for x in range(number_threads):
        thread = th.Thread(target=DW.main,args=(x, number_threads, conn_db))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()

