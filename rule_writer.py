import psycopg2
import time
import config


f = open(config.suricata_blacklist_file,"w")

conn_db = psycopg2.connect(f"dbname={config.db} user={config.user}")
cur = conn_db.cursor()
cur.execute(f"SELECT id,sha1,groupe,rev FROM {config.suricata_table};")
data = cur.fetchall()
to_write_in_file = ""
for elt in data:
    (sid,sha1,groupe,rev) = elt[0],elt[1],elt[2],elt[3]
    rule = f'drop tls $EXTERNAL_NET 443 -> $HOME_NET any (msg:"dropping tls connection"; tls.fingerprint:{sha1}; reference url, "url ?"; sid:"{sid}") \n'
    to_write_in_file += rule
f.write(to_write_in_file)
