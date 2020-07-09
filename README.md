# trial-extract-script

We have 2 versions of the trial-extract-script:
1. new_trials_insert_v1: Inserts records into the local server/ database one at a time by fetching it from the aact database. There is also an option to batch_write the records into our local database which is faster than writing records one by one.
2. new_trials_insert_v2: Generating a csv file for the clinical trial records which can be easy to read and understand. Also helpful when the data is huge.


There is no need to worry about having any duplicate records, even if by mistake we try to fetch trial records which are already existing. Such records will be filtered out. Hence, an input of overlapping dates will not generate duplicate records.


1 . For insertion of records one by one into table for the specified time range

python new_trials_insert_v1.py --username_aact __ --password_aact __ --hostname __ --database_name __ --username_local __ --password_local --from_date 2020-1-1 --to_date 2020-1-3  --table_name __

2. For generating a csv file for the specified time range

python new_trials_insert_v2.py --username_aact __ --password_aact __ --hostname __ --database_name __ --username_local __ --password_local __ --from_date 2020-5-1 --to_date 2020-5-2  --table_name __ --path to_save_csv_path


Parser variables and arguments:

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
