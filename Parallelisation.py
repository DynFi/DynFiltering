import multiprocessing as mp
import DatabaseWriter as DW
import Threading as Th
import time
import psycopg2

DATABASE = "testDB"
USER = ""

def main():    
    """
    Function to parallelize the DatabaseWriter main() function  
    Best is to use the Threading.py because Python can't handle multiple cores
    Maybe try mpi4py for splitting the database in each core then computing with multiple threads
    """
    conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    print("Number of processors: ", mp.cpu_count())
    tic = time.perf_counter()
    pool = mp.Pool(mp.cpu_count())
    pool.apply(Th.main(conn_db))               #Not sure if this works
    print("This is a Test")
    toc = time.perf_counter()
    print(f"Time Elapsed: {toc - tic}")
    pool.close()
    conn_db.close()


if __name__ == "__main__":
    main()

