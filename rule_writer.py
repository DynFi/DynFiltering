import psycopg2
import time

FILE = "rules_exemple"
DATABASE = "testDB"
USER = ""

f = open(FILE,"w")

conn_db = psycopg2.connect(f"dbname={DATABASE} user={USER}")
cur = conn_db.cursor()
cur.execute("SELECT DISTINCT sha1 from (SELECT sha1 FROM testtable4 WHERE sha1 IS NOT NULL) t;")
to_write_in_file = ""
list_sha1 = cur.fetchall()
for elt in list_sha1:
    sha1 = elt[0]
    rule = f'drop tls $EXTERNAL_NET -> $HOME_NET any (msg:"dropping tls connection"; tls.fingerprint:{sha1}; reference url, "url ?"; sid:"qqch"; rev:"je sais pas") \n'
    to_write_in_file += rule
f.write()
