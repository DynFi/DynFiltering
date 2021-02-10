import hashlib
import ssl
from socket import *
import psycopg2

PORT = 443
USER = ""  #Change to user
DATABASE = "testDB"
TABLE = "testtable4"



def retrieve_footprint_from_url(url, port):

    """
    Retrieve the SHA1 footprint from a website by 
    creating a TLS connection with a given URL
    """

    conn = ssl.create_connection((url, port),timeout=1)     #Make a connection
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    sock = context.wrap_socket(conn, server_hostname=url)   #Makes the connection TLS
    certificate = sock.getpeercert(True)                    #True gets the 'der' certificate 
    return hashlib.sha1(certificate).hexdigest()

def main():

    """
    Connect to the testDB postgresql Database
    retrieves each URL and computes the SHA1 footprint 
    to stock it back into the database 
    """

    for n in range(0,1000,100):                             #The loop is not the final version of the code but is for testing the code
        conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
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
                #pass
                print("Error")                             #Same here
                
        print(n)                                           #Same here
        conn_db.commit()
        cur.close()
        conn_db.close()

if __name__ == "__main__":
    main()
