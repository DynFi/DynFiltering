import psycopg2
import config
from datetime import datetime
import os

PATH = '/home/rmajoor/BL'

def get_group(sub_path,group_name):
    out = []
    with open('%s/domains'%sub_path,encoding='ISO-8859-1') as f:
        for line in f:
            line = line.strip()
            out.append([line,group_name])
    with open('%s/urls'%sub_path,encoding='ISO-8859-1') as f:
        for line in f:
            loc = line.find("://")
            if loc>0:
                line = line[0:loc+3]+line.split("/")[2]
            elif "/" in line:
                line = line.split("/")[0]
            line = line.strip()
            out.append([line,group_name])
    return out

def collect():
    group_dict = {}
    path = PATH
    groups = set(os.listdir(path))
    groups.remove('COPYRIGHT')
    groups.remove('global_usage')
        
    file_out = []
    for i,g in enumerate(groups):
        sub_path = '%s/%s'%(path,g)
        sub_groups = set(os.listdir(sub_path))
        if 'domains' in sub_groups:
            if g not in group_dict:
                group_dict[g] = len(group_dict)+1
            g_list = get_group(sub_path,g)
            file_out+=g_list
        else:
            for gg in sub_groups:
                if gg not in group_dict:
                    group_dict[gg] = len(group_dict)+1
                group_name = "%s-%s"%(g,gg)
                sub_sub_path = '%s/%s'%(sub_path,gg)
                g_list = get_group(sub_sub_path,group_name)
                file_out+=g_list
    return group_dict,file_out


def create_blacklist_dynfi():
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Database opened sucessfully")
        cur = con.cursor()
        try:
            cur.execute(f'''DROP TABLE {config.groupe_table};''')
            cur.execute(f'''DROP TABLE {config.main_table};''')
            print("Drop tables Successful")
        except Exception as e:
            print(e)
            pass
        cur.execute(f'''CREATE TABLE {config.main_table}
            (ID SERIAL PRIMARY KEY      NOT NULL,
            URL           VARCHAR(2048)      NOT NULL,
            GROUPE            INT       NOT NULL,
            SHA1        CHAR(40),
            CERTTIME      TIMESTAMP,
            TIMESTAMP         TIMESTAMP,
            ERROR       VARCHAR(100));''')
        print("Blacklist_Dynfi Table created successfully")

        #cur.execute(f"CREATE INDEX b_tree_index ON {config.main_table} USING btree (ID)")
        #print("B_tree index creation successfull")
        cur.execute(f'''CREATE TABLE {config.groupe_table}
            (ID INT PRIMARY KEY      NOT NULL,
            GROUPE     VARCHAR(50)   NOT NULL);''')
        print("Groupe table Successful")

        print("B_tree on groupe_table succesfull")
        con.commit()
        con.close()
    except:
        print("Database connection or table creation did not succeed")

    group_dict,file_out = collect();
    try: 
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Connection Successful")
        cur=con.cursor()

        for key,value in group_dict.items():
            #print(f"key: {key}, value:{value}")
            cur.execute(f"INSERT INTO {config.groupe_table} (ID,GROUPE) VALUES ({value},'{key}');");
        con.commit()
        con.close()
    except:
        print("Groupe database failed")
    #lastf = []
    try:
        con = psycopg2.connect(f"dbname={config.db} user={config.user}")
        print("Connection Successful")
        cur=con.cursor()
        
        for i,f in enumerate(file_out):
            try:
                val = str(f[0])
                if "'" not in val:
                    cur.execute(f"INSERT INTO {config.main_table} (URL,GROUPE) VALUES ('{val}',{group_dict[f[1]]});");

            except Exception as e:
                print(e)
            #    pass
        con.commit()
        con.close()
    except Exception as e:
        print(e)
        #print(lastf)
        print("Main Table failed")


if __name__ == "__main__":
    create_blacklist_dynfi()


