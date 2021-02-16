import hashlib
import ssl
from socket import *
import psycopg2
import time
import threading as th

PORT = 443
USER = ""  #Works without specifying a user 
DATABASE = "testDB" #Change with more explicit name
TABLE = "testtable4"   # Same here
SIZE_DB = 10000    #This should be retrieved from the DB itself instead of putting it as variable
REAL_DB_SIZE = 1664208
NUMBER_THREADS = 700     #Much time is lost with connections so nbr_threads must be between 100 and 1000 for optimized parsing

def multithreading():
    """
    Creates threads on DatabaseWriter for optimized parsing
    """

    tic = time.perf_counter()
    conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    threads = []

    print(f"Create threads: {NUMBER_THREADS} ")
    for x in range(NUMBER_THREADS):
        thread = th.Thread(target=main,args=(x, NUMBER_THREADS, conn_db))
        threads.append(thread)
        thread.start()

    print("Threads Started")
    for thread in threads:
        thread.join()

    toc = time.perf_counter()
    print(f"Time Elapsed: {toc - tic}")
    print(f"Performance: with {NUMBER_THREADS} threads: { SIZE_DB // ( toc - tic ) } urls per second")
    print(f"Performance: which means that the actual database is parsed in {REAL_DB_SIZE *(toc - tic) // (SIZE_DB*3600)} hours.")



def retrieve_footprint_from_url(url, port):

    """
    Retrieve the SHA1 footprint from a website by creating a
    TLS connection with a given URL.
    """

    conn = ssl.create_connection((url, port),timeout=1)     #Makes a connection
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Wrap the conncetion with TLS protocol
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate (easier for comptuting the certificate)
    return hashlib.sha1(certificate).hexdigest()

def main(Me = 1, number_thread = 1, conn_db=None):

    """
    Connect to the testDB postgresql Database retrieves each 
    URL and computes the SHA1 footprint to stock it back into
    the database.
    """
    loop_size = 5
    for n in range((SIZE_DB//number_thread)*(Me), (SIZE_DB//number_thread)*(Me+1), loop_size):  #Check wether the loop is correct

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
