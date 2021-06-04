from flask import Flask, render_template, request, send_file
from flask_celery import make_celery
from celery.schedules import crontab

import psycopg2
import jinja2
import sys
import ctypes

sys.path.append('../')
import config

sys.path.append(config.DF_path)
import Database_Filler as DF
from rule_writer import suricata_rule_writer

GROUPE_DB = config.groupe_database
DB = config.db
USER = config.user
FILE_PATH = config.suricata_blacklist_file

libgcc_s = ctypes.CDLL('libgcc_s.so.1')

#Creating Flask app and Celery
app=Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'pyamqp://localhost//'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'
app.config['CELERY_TIMEZONE'] = 'Europe/Paris'
celery = make_celery(app)

#This function is supposed to update the databases each Monday at 00:00 but i'm not sure if this really works
@celery.on_after_configure.connect
def setup_periodic_task(sender, **kwargs):
    sender.add_periodic_task(crontab(hour=0,minute=0,day_of_week=1),cron_function.s(),)

#Connects to the db
def get_db_connection():
    con = psycopg2.connect(f"dbname={DB} user={USER}")
    cur = con.cursor()
    return con,cur

#End db connection
def close_db_connection(con):
    con.commit()
    con.close()

#The flask application
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

#The celery app
@celery.task(name="cron_function")
def cron_function():
    DF.main()

