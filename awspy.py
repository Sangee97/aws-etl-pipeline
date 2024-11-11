import glob as glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import boto3
import mysql.connector
from sqlalchemy import create_engine

# Create an S3 client
s3 = boto3.client('s3',
                  aws_access_key_id='',
                  aws_secret_access_key='',
                  region_name='us-east-2')
#uploading raw data files into s3 bucket

file_name = "/Users/Sangeetha/Downloads/source1.csv"
bucket_name = 'test-bucket-project4'
object_name = 's1.csv'
s3.upload_file(file_name, bucket_name, object_name)
file_name = '/Users/Sangeetha/Downloads/source1.xml'
bucket_name = 'test-bucket-project4'
object_name = 's1.xml'
s3.upload_file(file_name, bucket_name, object_name)
file_name = "/Users/Sangeetha/Downloads/source1.json"
bucket_name = 'test-bucket-project4'
object_name = 's1.json'
s3.upload_file(file_name, bucket_name, object_name)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now()) + ":uploading raw source1 data files into s3 bucket.\n")
file_name = "/Users/Sangeetha/Downloads/source2.csv"
bucket_name = 'test-bucket-project4'
object_name = 's2.csv'
s3.upload_file(file_name, bucket_name, object_name)
file_name = '/Users/Sangeetha/Downloads/source2.xml'
bucket_name = 'test-bucket-project4'
object_name = 's2.xml'
s3.upload_file(file_name, bucket_name, object_name)
file_name = "/Users/Sangeetha/Downloads/source2.json"
bucket_name = 'test-bucket-project4'
object_name = 's2.json'
s3.upload_file(file_name, bucket_name, object_name)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now()) + ":uploading raw source2 data files into s3 bucket.\n")

file_name = "/Users/Sangeetha/Downloads/source3.csv"
bucket_name = 'test-bucket-project4'
object_name = 's3.csv'
s3.upload_file(file_name, bucket_name, object_name)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now()) + ":uploading raw source3 data files into s3 bucket.\n")
file_name = '/Users/Sangeetha/Downloads/source3.xml'
bucket_name = 'test-bucket-project4'
object_name = 's3.xml'
s3.upload_file(file_name, bucket_name, object_name)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now()) + ":uploading raw source3 data files into s3 bucket.\n")
file_name = "/Users/Sangeetha/Downloads/source3.json"
bucket_name = 'test-bucket-project4'
object_name = 's3.json'
s3.upload_file(file_name, bucket_name, object_name)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now()) + ":uploading raw source3 data files into s3 bucket.\n")

print("File uploaded successfully!")

##EXTRACTING THE DATA FROM VARIOUS FILES (CONTAINING DIFFERENT FILE FORMATS):

#reading only csv file and convert it into single dataframe

csv_files=glob.glob("/Users/Sangeetha/Desktop/source/*.csv")

df=pd.DataFrame() #empty dataframe
for csv_file in csv_files:
    with open("my_log.txt", "a") as log_file:
        log_file.write(str(datetime.now()) + ":Extracting csv_files.\n")
    temp_df=pd.read_csv(csv_file) #temperory df read the csv file
    df= pd.concat([df,temp_df])#concat the temperory df with df


#reading json file and joining in single dataframe
json_files=glob.glob("/Users/Sangeetha/Desktop/source/*.json")

for json_file in json_files:
    with open("my_log.txt", "a") as log_file:
        log_file.write(str(datetime.now()) + ":Extracting JSON_files.\n")

    tmp_df=pd.read_json(json_file,lines=True)# json files are one by one
    df = pd.concat([df,tmp_df])


# reading and parsing xml files and combining with single dataframe
xml_files=glob.glob("/Users/Sangeetha/Desktop/source/*.xml")

list = []

#parsing the xml file using parse() function in elementtree module
for xml_file in xml_files:
    with open("my_log.txt", "a") as log_file:
        log_file.write(str(datetime.now()) + ":Extracting XML_files.\n")
    tree = ET.parse(xml_file)
    root = tree.getroot()
#need to extract the data from the XML file by iterating over the XML tree, accessing the tags and text of each element
    for elem in root.findall('.//person'):
        dict = {}
        for child in elem:
            dict[child.tag] = child.text
        list.append(dict)

temp2_df=pd.DataFrame(list)
df = pd.concat([df,temp2_df])

with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now())+":extracted all the files from source folder.\n")

##TRANSFORMING THE COLUMN VALUES INTO REQUIRED FORMAT:

# Step 1: Select the column
height_column = df['height']
weight_col=df['weight']

# Step 2: Apply a function to each value
def in_cm(x):
    # print(type(x))
    y=(int(float(x))*2.54)
    return round(y,1)


new_column = height_column.apply(in_cm)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now())+":transforming the height col from inch to cm.\n")

#step:3: apply function to change weight column:
def in_kgs(z):
    k=(int(float(z))*0.45)
    return round(k,1)
new2_column=weight_col.apply(in_kgs)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now())+"transforming weight col from lbs to kgs.\n")

# Step 3: Assign the new values back to the column
df['height'] = new_column
df['weight'] = new2_column

with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now())+":all files are transformed.\n")

# converting dataframe to csv file (loading)

df.to_csv('transformed_data.csv', index=False)
with open("my_log.txt", "a") as log_file:
    log_file.write(str(datetime.now())+":df is converted to csv file.\n")


# Import CSV
data = pd.read_csv(r"C:\Users\Sangeetha\Desktop\milestoneproject_1\transformed_data.csv")
df = pd.DataFrame(data)

file_name = "transformed_data.csv"
bucket_name = 'test-bucket-project4'
object_name = 'transformed_data1.csv'
s3.upload_file(file_name, bucket_name, object_name)

# Connect to local SQL Server
connection=mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="proj4"
)

cursor = connection.cursor()
cursor.execute('select * from transformed_data')
print(cursor.fetchall())


# load the data in rds

hostname=""
username='admin'
password='sangeetha'
port=3306
database='proj4'

print('mysql+pymysql://' +username+':'+password+'@'+hostname+':'+str(port)+'/'+database)
cnx=create_engine('mysql+pymysql://' +username+':'+password+'@'+hostname+':'+str(port)+'/'+database)
conn=cnx.connect()

sql_query=pd.read_sql_query('select * from proj4.transformed_data',conn)
df1=pd.DataFrame(sql_query)
#print(df1)





