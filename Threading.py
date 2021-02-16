import threading as th
import DatabaseWriter as DW
import time
import psycopg2

DATABASE = "testDB"
USER = ""


def main():
    """
    Creates threads on DatabaseWriter for optimized parsing
    """
    tic = time.perf_counter()
    conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    number_threads = 1000
    threads = []
    for x in range(number_threads):
        thread = th.Thread(target=DW.main,args=(x, number_threads, conn_db))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    toc = time.perf_counter()
    print(f"Time Elapsed: {toc - tic}")


if __name__ == "__main__":
    main()

