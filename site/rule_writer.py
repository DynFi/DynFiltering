import psycopg2
import time
import config

def suricata_rule_writer(groupe_names):
    f = open(config.suricata_blacklist_file,"w")
    groupe_numbers = []

    conn_db = psycopg2.connect(f"dbname={config.db} user={config.user}")
    cur = conn_db.cursor()

    for name in groupe_names:
        cur.execute(f"SELECT id FROM {config.groupe_table} WHERE groupe='{name}';")
        number = cur.fetchall()
        print(number[0][0])
        groupe_numbers.append(number[0][0])


    cur.execute(f"SELECT id,sha1,groupe,rev FROM {config.suricata_table};")
    data = cur.fetchall()
    to_write_in_file = ""
    for elt in data:
        (sid,sha1,groupe,rev) = elt[0],elt[1],elt[2],elt[3]
        if groupe in groupe_numbers:
            print(groupe)
        if groupe in groupe_numbers:
            rule = f'drop tls $EXTERNAL_NET 443 -> $HOME_NET any (msg:"dropping tls connection"; tls.fingerprint:"{sha1}"; reference url, "url ?"; sid:"{sid}") \n'
            to_write_in_file += rule
    f.write(to_write_in_file)

if __name__ == "__main__":
    groupe_names = ['porn', 'hospital']
    suricata_rule_writer(groupe_names)

