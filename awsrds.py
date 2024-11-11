from sqlalchemy import create_engine
import pandas as pd

hostname='database1.cf6myuicex3n.us-east-2.rds.amazonaws.com'
username='admin'
password='sangeetha'
port=3306
database='proj4'
print('mysql+pymysql://' +username+':'+password+'@'+hostname+':'+str(port)+'/'+database)
cnx=create_engine('mysql+pymysql://' +username+':'+password+'@'+hostname+':'+str(port)+'/'+database)
conn=cnx.connect()
sql_query=pd.read_sql_query('select * from proj4.transformed_data',conn)
df=pd.DataFrame(sql_query)
print(df)

