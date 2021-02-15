import hashlib
import ssl
from socket import *
import psycopg2

PORT = 443
USER = ""  #Works without specifying a user 
DATABASE = "testDB" #Change with more explicit name
TABLE = "testtable4"   # Same here
SIZE_DB = 1664200 #This should be retrieved from the DB itself instead of putting it as variable



def retrieve_footprint_from_url(url, port):

    """
    Retrieve the SHA1 footprint from a website by creating a
    TLS connection with a given URL.
    """

    conn = ssl.create_connection((url, port),timeout=0.5)     #Makes a connection
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Wrap the conncetion with TLS protocol
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate (easier for comptuting the certificate)
    return hashlib.sha1(certificate).hexdigest()

def main(Me = 1, number_thread = 1, conn_db=None):

    """
    Connect to the testDB postgresql Database retrieves each 
    URL and computes the SHA1 footprint to stock it back into
    the database.
    
    This function works by running the script itself or by 
    running one of the scripts Threading.py or 
    Parallelisation.py.

    Use 'python3 Parallelisation.py' for best performanceson 
    this script.

    If a thread creation fails use 'python3 DatabaseWriter.py'
    directly.
    """

    for n in range((SIZE_DB//number_thread)*(Me), (SIZE_DB//number_thread)*(Me) + 1000, 100):  #Check wether the loop is correct

        #conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
        cur = conn_db.cursor()
        sqlQ = f"SELECT url,id FROM {TABLE} ORDER BY id LIMIT 100 OFFSET {n};"
        cur.execute(sqlQ)
        list_urls = cur.fetchall()

        for elt in list_urls:
            try:
                the_url = elt[0]
                the_id = elt[1]
                sha1_footprint = retrieve_footprint_from_url(the_url, PORT)
                print("Footprint: "+sha1_footprint)        #Decomment to debug the code
                print("Id: "+ str(the_id))                 #Same here
                cur.execute(f"UPDATE {TABLE} SET sha1='{sha1_footprint}' WHERE id={the_id};",(the_id,))

            except:
                pass
                #print("Error")                             #Same here
                
        print(n+100)                                           #Same here (Best is to keep this line uncommented for overviewing the good functionning of the script)
        conn_db.commit()
        cur.close()
        #conn_db.close()

if __name__ == "__main__":
    main()
