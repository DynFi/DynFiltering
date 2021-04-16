import psycopg2
import config
from datetime import datetime


def create_blacklist_dynfi():
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Database opened successfully")
        cur = con.cursor()
        try:
            cur.execute(f'''DROP TABLE {config.groupe_table};''')
            cur.execute(f'''DROP TABLE {config.main_table};''')
            print("Drop tables Successful")
        except Exception as e:
            print(e)
            pass
        cur.execute(f'''CREATE TABLE {config.main_table}
              (ID INT PRIMARY KEY    NOT NULL,
              URL           TEXT    NOT NULL,
              GROUPE            INT     NOT NULL,
              SHA1        CHAR(40),
              CERTTIME      TIMESTAMP,
              TIMESTAMP         TIMESTAMP,
              ERROR         VARCHAR(100));''')
        print("Blacklist_Dynfi Table created successfully")
        try:
            cur.execute(f"CREATE INDEX b_tree_idx ON {config.main_table} USING btree (url)")
            print("B_tree index creation successfull")
        except Exception as e:
            print(e)

        cur.execute(f'''CREATE TABLE {config.groupe_table}
                (ID INT PRIMARY KEY   NOT NULL,
                GROUPE      VARCHAR(50)    NOT NULL);''')
        print("Groupe table Successful")
        con.commit()
        con.close()
    except Exception as e:
        print(e)
        print("Database connection or table creation  did not succeed")

    try:
        group_dict = {}
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Connection Successful")
        cur = con.cursor()
        database_file = open(config.path_to_db,'r')
        content = database_file.readlines()
        print("Read files Successful")
        for i,line in enumerate(content):
            value = line.split(";")
            if value[-2] not in group_dict:
                group_dict[value[-2]] = len(group_dict)+1
        print("Dictionary Successful")
        for key,value in group_dict.items():
            print(f"key: {key}, value:{value}")
            cur.execute(f"INSERT INTO {config.groupe_table} (ID,GROUPE) VALUES ({value}, '{key}');");
        con.commit()
        con.close()
    except:
        print("Groupe database failed")
    
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        cur = con.cursor()
        database_file = open(config.path_to_db,'r')
        content = database_file.readlines()
        for i,line in enumerate(content):
            value = line.split(";")
            try:
                cur.execute(f"INSERT INTO {config.main_table} (ID,URL,GROUPE) VALUES ({i}, '{value[0]}', {group_dict[value[1]]});");
            except Exception as e:
                print(e)
                pass
        con.commit()
        con.close()
    except Exception as e:
        print(e)
        print(value)
        print(line)
        print("Main Table failed")

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
    create_blacklist_dynfi()
    #create_suricata_database()
    #fill_suricata_database()
