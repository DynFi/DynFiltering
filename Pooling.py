import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ssl
from socket import *
import config

import time
import threading as th

PORT = 443

USER = config.user
TABLE = config.main_table
DATABASE = config.db
PERF_FILE = config.perf_file
#USER = ""  #Works without specifying a user 
#DATABASE = "testDB" #Change with more explicit name
#TABLE = "testtable4"   # Same here

SIZE_DB = 1664208    #This should be retrieved from the DB itself instead of putting it as variable
REAL_DB_SIZE = 1664208
NUMBER_THREADS = 100     #Much time is lost with connections so nbr_threads must be between 100 and 1000 for optimized parsing
ELT_PER_TH = 10

def multithreading():
    tic = time.perf_counter()
    try:
        threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(1,NUMBER_THREADS,user=USER,database=DATABASE)
        if (threaded_postgreSQL_pool):
            print("Connection pool created successfully using ThreadedConnectionPool")
            
            
            with ThreadPoolExecutor(max_workers=NUMBER_THREADS) as pool:
                for n in range(SIZE_DB//ELT_PER_TH):
                    ret = pool.submit(wrapper, threaded_postgreSQL_pool, n, ELT_PER_TH)
                    #print ret.result()
                    #print("successfully recived connection from connection pool ")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL",error)

    finally:
        toc = time.perf_counter()
        print(f"Time Elapsed:{toc-tic}\nSize of Database:{SIZE_DB}\nSpeed:{SIZE_DB/(toc-tic)}\nThreads:{NUMBER_THREADS}")
        write_perf_into_file(PERF_FILE,toc-tic,SIZE_DB,NUMBER_THREADS)
        #closing database connection
        #use closeall() method to close all the active connection if you want to turn of the application
        if threaded_postgreSQL_pool:
            threaded_postgreSQL_pool.closeall
        #print("Threaded PostgreSQL connection pool is closed")

def retrieve_footprint_from_url(url, port):

    """
    Retrieve the SHA1 footprint from a website by creating a
    TLS connection with a given URL.
    """

    conn = ssl.create_connection((url, port),timeout=3)     #Makes a connection
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Wrap the connection with TLS protocol
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate (easier for comptuting the certificate)
    return hashlib.sha1(certificate).hexdigest()

def wrapper(tcp,n,elt_per_thread):
    ps_connection = tcp.getconn()
    if (ps_connection):
        main(n,ps_connection,elt_per_thread)
        tcp.putconn(ps_connection)


def write_perf_into_file(file_name,time_elapsed,size_db,nb_th):
    f = open(file_name,'a')
    perf = f"Time Elapsed: {time_elapsed} ; Size Database: {size_db} ; Overall Speed: {size_db/time_elapsed} ; Number of Threads: {nb_th}\n"
    f.write(perf)
    f.close()

def main(n,conn_db,elt_per_thread):
    #conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")    #This is not scalable because postgre doesnt allow more than 100 connections
    cur = conn_db.cursor()
    sqlQ = f"SELECT url,id FROM {TABLE} ORDER BY id LIMIT {elt_per_thread} OFFSET {n*elt_per_thread};" #Else memory is not big enough
    cur.execute(sqlQ)
    list_urls = cur.fetchall()

    for elt in list_urls:
        try:
            the_url = elt[0]
            the_id = elt[1]
            sha1_footprint = retrieve_footprint_from_url(the_url, PORT)
            #print("Footprint: "+sha1_footprint)        #Decomment to debug the code
            #print("Id: "+ str(the_id))                 #Same here
            cur.execute(f"UPDATE {TABLE} SET sha1='{sha1_footprint}' WHERE id={the_id};",(the_id,))

        except:
            pass
            #print("Error")                             #Same here
                
    #print(n+loop_size)                                 #Same here
    conn_db.commit()
    cur.close()
    #conn_db.close()

if __name__ == "__main__":
    #main()     #Uncomment if you do not want multithreading
    multithreading()

