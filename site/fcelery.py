from flask import Flask, render_template, request, send_file
from flask_celery import make_celery
from celery.schedules import crontab
#from flask_sqlalchemy import SQLAlchemy
#import config

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

import jinja2
from rule_writer import suricata_rule_writer

GROUPE_DB = config.groupe_database
FILE_PATH = config.suricata_blacklist_file

PORT = 403
USER = config.user
TABLE = config.main_table
DATABASE = config.db
HOST = config.host
PASSWORD = config.password
PERF_FILE = config.perf_file
NUMBER_THREADS = 500    #Much time is lost with connections so nbr_threads must be between 100 and 1000 for optimized parsing
ELT_PER_TH = 50       #Cannot be 1 else not enough memory (too much database requests in parallel)
libgcc_s = ctypes.CDLL('libgcc_s.so.1')

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'pyamqp://localhost//'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'
app.config['CELERY_TIMEZONE'] = 'Europe/Paris'


celery = make_celery(app)

@celery.on_after_configure.connect
def setup_periodic_task(sender, **kwargs):
    sender.add_periodic_task(crontab(hour=16,minute=36,day_of_week=6),main.s(),)

def get_table_size(db,user,table):
    """
    Retrieve the size of the given postgre table
    """
    conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    cur = conn_db.cursor()
    cur.execute(f"SELECT count(*) FROM {table};")
    size = cur.fetchall()[0][0]
    cur.close
    return size

SIZE_DB = get_table_size(DATABASE,USER,TABLE)

@celery.task(name="main")
def main():
    from celery import current_task
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

def retrieve_footprint_from_url(url, port):
    """
    Retrieve the SHA1 footprint from a website by creating a
    TLS connection with a given URL.
    """
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

def write_perf_into_file(file_name,time_elapsed,size_db,nb_th):
    f = open(file_name,'a')
    perf = f"Time Elapsed: {time_elapsed} ; Size Database: {size_db} ; Overall Speed: {size_db/time_elapsed} ; Number of Threads: {nb_th}\n"
    f.write(perf)
    f.close()

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

def get_db_connection():
    con = psycopg2.connect(f"dbname={DATABASE} user={USER}")
    cur = con.cursor()
    return con,cur

def close_db_connection(con):
    con.commit()
    con.close()

@app.route('/',methods=['GET', 'POST'])
def hello():
    con,cur = get_db_connection()
    cur.execute(f"SELECT distinct(GROUPE) FROM {GROUPE_DB};")
    data = cur.fetchall()
    nice_data = []
    for elt in data:
        nice_data.append(elt[0])
    close_db_connection(con)
    if request.method == "POST":
        groupes = request.form.getlist('groupe')
        suricata_rule_writer(groupes)
        return send_file(FILE_PATH, as_attachment=True)
    return render_template("get_template.jinja2",data=nice_data)

@app.route('/usecelery',methods=['GET'])
def usecelery():
    main.delay()
    return render_template("get_template.jinja2")


