import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ssl
from socket import *

import time
import threading as th

PORT = 443
USER = ""  #Works without specifying a user 
DATABASE = "testDB" #Change with more explicit name
TABLE = "testtable4"   # Same here
SIZE_DB = 1000    #This should be retrieved from the DB itself instead of putting it as variable
REAL_DB_SIZE = 1664208
NUMBER_THREADS = 100     #Much time is lost with connections so nbr_threads must be between 100 and 1000 for optimized parsing

def multithreading():
    tic = time.perf_counter()
    try:
        threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(1,NUMBER_THREADS,user=USER,database=DATABASE)
        if (threaded_postgreSQL_pool):
            #print("Connection pool created successfully using ThreadedConnectionPool")
            
            
            with ThreadPoolExecutor(max_workers=50) as pool:
                for n in range(SIZE_DB):
                    ret = pool.submit(wrapper, threaded_postgreSQL_pool,n)
                    #print ret.result()
                    #print("successfully recived connection from connection pool ")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL",error)

    finally:
        toc = time.perf_counter()
        print(f"{toc-tic},{SIZE_DB}")
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

    conn = ssl.create_connection((url, port),timeout=20)     #Makes a connection
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Wrap the conncetion with TLS protocol
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate (easier for comptuting the certificate)
    return hashlib.sha1(certificate).hexdigest()

def wrapper(tcp,n):
    loop_size = 1
    ps_connection = tcp.getconn()
    if (ps_connection):
        main(n,loop_size,ps_connection)
        tcp.putconn(ps_connection)


def main(n,loop_size,conn_db):
        #conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")    #This is not scalable because postgre doesnt allow more than 100 connections
    cur = conn_db.cursor()
    sqlQ = f"SELECT url,id FROM {TABLE} ORDER BY id LIMIT {loop_size} OFFSET {n};"
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

