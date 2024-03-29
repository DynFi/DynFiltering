import psycopg2
import config
from datetime import datetime

'''
The function creates two databases one for the group_names and assigning them group_indexes, and the other for the url, group_index, and eventually sha1, cert_time, timestamp and error.  
'''
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


if __name__ == "__main__":
    create_blacklist_dynfi()
   
