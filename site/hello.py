from flask import Flask, render_template, request, send_file
import psycopg2
import config
import jinja2
from rule_writer import suricata_rule_writer

GROUPE_DB = config.groupe_database
DB = config.db
USER = config.user
FILE_PATH = config.suricata_blacklist_file

app=Flask(__name__)

def get_db_connection():
    con = psycopg2.connect(f"dbname={DB} user={USER}")
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
