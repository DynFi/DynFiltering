import multiprocessing as mp
import DatabaseWriter as DW


def main():      #Before extending this function to more CPUs, check if it really improves the speed
    """
    Function to parallelize the DatabaseWriter main() function
    """
    print("Number of processors: ", mp.cpu_count())
    pool = mp.Pool(mp.cpu_count())
    pool.apply(DW.main())               #Not sure if this really works
    pool.close()


if __name__ == "__main__":
    main()

