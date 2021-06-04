from datetime import datetime
import psycopg2
import sys

sys.path.append('../')
import config


'''
Creates the suricata database which will just be a simplified version of the database specified in config.main_table that will be used to create the suricata rules. (group, sha1, rev)
'''
def create_suricata_database():
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Database opened successfully")
    except Exception as e:
        print(e)
    cur = con.cursor()
    try:
        cur.execute(f'''DROP TABLE {config.suricata_table};''')
    except Exception as e:
        pass
    try:
        cur.execute(f'''CREATE TABLE {config.suricata_table}
              (ID INT PRIMARY KEY    NOT NULL,
              GROUPE            INT     NOT NULL,
              SHA1        CHAR(40)   NOT NULL,
              REV       INT);''')
        print("Suricata Table created successfully")
        con.commit()
        con.close()
    except Exception as e:
        print("here")
        print(e)
'''
fill the suricata database by only taking one copy of a given sha1 certificate, if the databases contained multiple urls with the same certificate and only certificates that have not yet expired.
'''
def fill_suricata_database():
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Database opened successfully")
        cur = con.cursor()
        todays_date = str(datetime.now())[:10].replace('-','')
        print(todays_date)
        cur.execute(f"SELECT DISTINCT sha1,groupe FROM (SELECT sha1,groupe FROM {config.main_table} WHERE sha1 is not null AND certtime > TO_DATE('{todays_date}','YYYYMMDD')) AS t;")
        data = cur.fetchall()
        for i,line in enumerate(data):
            sha1 = line[0]
            groupe = line[1]
            cur.execute(f"INSERT INTO {config.suricata_table} (ID,GROUPE,SHA1) VALUES ({i}, {groupe}, '{sha1}');");
        con.commit()
        con.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    create_suricata_database()
    fill_suricata_database()
