import multiprocessing as mp
import DatabaseWriter as DW
import Threading as Th
import time

def main():      #Before extending this function to more CPUs, check if it really improves the speed
    """
    Function to parallelize the DatabaseWriter main() function
    """
    print("Number of processors: ", mp.cpu_count())
    tic = time.perf_counter()
    pool = mp.Pool(mp.cpu_count())
    pool.apply(Th.main())               #Not sure if this really works
    toc = time.perf_counter()
    print(f"Time Elapsed: {toc - tic}")
    pool.close()


if __name__ == "__main__":
    main()

