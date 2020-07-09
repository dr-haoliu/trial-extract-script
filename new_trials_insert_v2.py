import pandas as pd
import numpy as np
import datetime

import psycopg2
import mysql.connector
from mysql.connector import Error

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-u","--username_aact", help = "give aact database username")
parser.add_argument("-p","--password_aact", help = "give aact database password")
parser.add_argument("-hn","--hostname", help = "give local or server hostname")
parser.add_argument("-d","--database_name", help = "givelocal database name")
parser.add_argument("-us","--username_local", help = "give local database username")
parser.add_argument("-ps","--password_local", help = "give local database password")
parser.add_argument("-f","--from_date", help = "give start date for trials extraction")
parser.add_argument("-t","--to_date", help = "give end date for trials extraction")
parser.add_argument("-ta","--table_name", help = "give name of your local table")
parser.add_argument("-pa","--path", help = "Enter path where you want to save the csv file")

args = parser.parse_args()

connection1 = psycopg2.connect(
    user=args.username_aact,
    password=args.password_aact,
    database="aact",
    host="aact-db.ctti-clinicaltrials.org")

connection1.set_client_encoding('utf-8')

cursor1 = connection1.cursor()


connection2 = mysql.connector.connect(host=args.hostname,
                                      database=args.database_name, # put the name of your local database where you have the tables
                                      user=args.username_local,
                                      charset='utf8',
                                      password=args.password_local)

cursor2 = connection2.cursor()

#print(args.username_aact,args.password_aact,args.hostname,args.database_name,args.username_local,args.password_local,args.from_date,args.to_date)

x = args.from_date.split("-")
y = args.to_date.split("-")
table=args.table_name
# specify the time range for record extraction in YY-MM-DD format
from_date = datetime.date(int(x[0]),int(x[1]), int(x[2]))
till_date = datetime.date(int(y[0]),int(y[1]), int(y[2]))
cursor2.execute("select nct_id from "+table) # name of your table 
rows = cursor2.fetchall()
nct_id_list_local = []
for r in rows:
    nct_id_list_local.append(r[0])


cursor1.execute("Select nct_id from studies where study_first_submitted_date BETWEEN '" + str(from_date) + "' AND '" + str(till_date) + "'")
result = cursor1.fetchall()
nct_id_list_aact = []
for r in result:
    nct_id_list_aact.append(r[0])

final_nct_list = list(set(nct_id_list_aact) - set(nct_id_list_local))
# finding the last id in the local table

cursor2.execute("select id from " +table+ " where id = (select MAX(id) from "+table+")") 
last_id = cursor2.fetchall()[0][0]

data = []
for ids in final_nct_list:
    cursor1.execute(
        "Select study_type, brief_title, official_title,overall_status,phase,start_date,primary_completion_date,completion_date from studies where nct_id = '" + ids + "'")
    res1 = cursor1.fetchall()
    cursor1.execute(
        "Select gender, minimum_age,maximum_age,healthy_volunteers from eligibilities where nct_id = '" + ids + "'")
    res2 = cursor1.fetchall()
    cursor1.execute("Select city ,state, zip, country from facilities where nct_id = '" + ids + "'")
    res3 = cursor1.fetchall()
    last_id += 1
    
    study_type = ""
    brief_title = ""
    official_title = ""
    overall_status = ""
    phase = ""
    gender = ""
    minimum_age = ""
    maximum_age = ""
    healthy_volunteers = ""
    city = ""
    state = ""
    country = ""
    zipcode = ""
  
    for j in res1:
        study_type += str(j[0])
        brief_title += str(j[1])
        official_title += str(j[2])
        overall_status += str(j[3])
        phase += str(j[4])
        start_date = j[5]
        primary_completion_date = j[6]
        completion_date = j[7]

    for k in res2:
        gender += str(k[0])
        minimum_age += str(k[1])
        maximum_age += str(k[2])
        healthy_volunteers += str(k[3])

    if len(res3) == 0:
        data.append(tuple([last_id, ids, study_type, brief_title, official_title, overall_status, phase, str(start_date),
                      str(primary_completion_date), str(completion_date), gender, minimum_age, maximum_age,
                      healthy_volunteers, "", "", "", ""]))
    else:
        for l in res3:
            if str(l[0]) not in city and str(l[0]) != "":
                city += str(l[0]) + "|"
            if str(l[1]) not in state and str(l[1]) != "":
                state += str(l[1]) + "|"
            if str(l[2]) not in zipcode and str(l[2]) != "":
                zipcode += str(l[2]) + "|"
            if str(l[3]) not in country and str(l[3]) != "":
                country += str(l[3]) + "|"

        data.append(tuple([last_id, ids, study_type, brief_title, official_title, overall_status, phase, str(start_date),
                      str(primary_completion_date), str(completion_date), gender, minimum_age, maximum_age,
                      healthy_volunteers, city[:-1], state[:-1], zipcode[:-1], country[:-1]]))
    print(tuple([last_id, ids, study_type, brief_title, official_title, overall_status, phase, str(start_date),
                      str(primary_completion_date), str(completion_date), gender, minimum_age, maximum_age,
                      healthy_volunteers, city[:-1], state[:-1], zipcode[:-1], country[:-1]]))
    

df=pd.DataFrame(data, columns = ['id','nct_id' , 'study_type' ,'brief_title','official_title', 'overall_status','phase','start_date','primary_completion_date','completion_date','gender','minimum_age','maximum_age','healthy_volunteers', 'city' ,'state','zipcode', 'country']) 


df.to_csv(args.path,index=False)

cursor1.close()
connection1.close()
cursor2.close()
connection2.close()
