import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ssl
from socket import *
import config
import OpenSSL
from datetime import datetime
import time
import threading as th
import ctypes



#Definition of all the parameters, read the config_example.py file for more information

PORT = 443
USER = config.user
TABLE = config.main_table #specify the database in the config file
DATABASE = config.db
PERF_FILE = config.perf_file
NUMBER_THREADS = 500    #Much time is lost with connections so nbr_threads must be between 100 and 1000 for optimized parsing
ELT_PER_TH = 50       #Cannot be 1 else not enough memory (too much database requests in parallel)
libgcc_s = ctypes.CDLL('libgcc_s.so.1')

'''
Get the size of the given postgres table
'''
def get_table_size(db,user,table):
    conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    cur = conn_db.cursor()
    cur.execute(f"SELECT count(*) FROM {table};")
    size = cur.fetchall()[0][0]
    cur.close
    return size

SIZE_DB = get_table_size(DATABASE,USER,TABLE)

'''
Create a thread pool, run thread function to fill the table, allocate number of urls to be treated by each thread. 
'''
def main():
    tic = time.perf_counter()
    print(f"Size of table : {SIZE_DB}")
    try:
        threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(1,NUMBER_THREADS,user=USER,database=DATABASE)
        if (threaded_postgreSQL_pool):
            print("Connection pool created successfully using ThreadedConnectionPool")
            with ThreadPoolExecutor(max_workers=NUMBER_THREADS) as pool:
                for n in range(887000//ELT_PER_TH, SIZE_DB//ELT_PER_TH):
                    ret = pool.submit(thread, threaded_postgreSQL_pool, n, ELT_PER_TH)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL",error)

    finally:
        toc = time.perf_counter()
        print(f"Time Elapsed:{toc-tic}\nSize of Database:{SIZE_DB}\nSpeed:{SIZE_DB/(toc-tic)}\nThreads:{NUMBER_THREADS}")
        write_perf_into_file(PERF_FILE,toc-tic,SIZE_DB,NUMBER_THREADS)
        if threaded_postgreSQL_pool:
            threaded_postgreSQL_pool.closeall

'''
Retrieve the SHA1 footprint from a website by creating a TLS connection with
a given URL on a given port. 
'''
def retrieve_footprint_from_url(url, port):
    conn = ssl.create_connection((url, port),timeout=10)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Wrap the connection with TLS protocol
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate (easier for computing the certificate) 
    PEM_cert = ssl.DER_cert_to_PEM_cert(certificate)
    try:
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, PEM_cert)
        cert_not_after = datetime.strptime(x509.get_notAfter().decode('ascii'), '%Y%m%d%H%M%SZ')
    except Exception as e:
        pass
        #print(e)
    return (hashlib.sha1(certificate).hexdigest(),str(cert_not_after)[:10])

'''
For testing purposes write performance of main function to a given file_name
performation includes time_elapsed to fill the table, the size of the table 
filled, and the number of threads used. 
'''
def write_perf_into_file(file_name,time_elapsed,size_db,nb_th):
    f = open(file_name,'a')
    perf = f"Time Elapsed: {time_elapsed} ; Size Database: {size_db} ; Overall Speed: {size_db/time_elapsed} ; Number of Threads: {nb_th}\n"
    f.write(perf)
    f.close()

'''
Get a thread from a threadpool establish a connection to the database. Retrievesthe next set of urls: elt_per_thread and n determines which urls have already been treated and finds the next set of urls to be treated by this thread. Use the function retrieve_footprint_from_url to get the SHA1 footprint and its expiration date, or an error and update the table.
'''
def thread(threaded_postgresql_pool,n,elt_per_thread):
    conn_db = threaded_postgresql_pool.getconn()
    if (conn_db):
        cur = conn_db.cursor()
        sqlQ = f"SELECT url,id FROM {TABLE} ORDER BY id LIMIT {elt_per_thread} OFFSET {n*elt_per_thread};" #Else memory is not big enough
        cur.execute(sqlQ)
        list_urls = cur.fetchall()
        for elt in list_urls:

            the_url = elt[0]
            the_id = elt[1]

            try:
                if int(the_id)%1000 == 0 :
                    print(f"Line number {the_id} has been completed.")
                (sha1_footprint,cert_time) = retrieve_footprint_from_url(the_url, PORT)
                timestamp = str(datetime.now())
                cur.execute(f"UPDATE {TABLE} SET sha1='{sha1_footprint}',certtime='{cert_time}',timestamp='{timestamp}' WHERE id={the_id};")

            except Exception as e:
                cur.execute(f"UPDATE {TABLE} SET error='{e}' WHERE id={the_id};")
                #print(e)
                pass

        conn_db.commit()
        cur.close()
        #conn_db.close()
        threaded_postgresql_pool.putconn(conn_db)

if __name__ == "__main__":
    main()

