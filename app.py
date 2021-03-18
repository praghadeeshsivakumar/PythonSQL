import pymysql
import csv
import datetime
import threading
import time
import logging
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime


env_path = Path('D:/TA_Project/param.env')
load_dotenv(env_path)
from sqlalchemy import create_engine,Table, Column, Integer, String, MetaData, Float, Date, BigInteger, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import *


engine = create_engine('mysql+pymysql://root:SQL500@localhost:3306/sales')
meta = MetaData()
hr = Table(
   'hrtable', meta, 
   Column('id', Integer, primary_key = True), 
   Column('Region', String(200)), 
   Column('Country', String(200)), 
   Column('Item_Type', String(20)), 
   Column('Sales_Channel', String(20)), 
   Column('Order_Priority', String(20)), 
   Column('Order_Date', Date), 
   Column('Order_ID', BigInteger), 
   Column('Ship_Date', Date), 
   Column('Units_Sold', Integer), 
   Column('Unit_Price', Float),
   Column('Unit_Cost', Float), 
   Column('Total_Revenue', Float),
   Column('Total_Cost', Float), 
   Column('Total_Profit', Float),
)
#a = meta.create_all(engine)
#print(a)
Base = declarative_base()
def loaddata():
   insert_query = hr.insert()
   with open('C:\\Users\\Praghadeesh\\Downloads\\5kSalesRec.csv', 'r', encoding="utf-8") as csvfile:
      csv_reader = csv.reader(csvfile, delimiter=',')
      next(csv_reader)
      engine.execute(
         insert_query,
         [{"Region": row[0], "Country": row[1], "Item_Type": row[2], "Sales_Channel": row[3], "Order_Priority": row[4], "Order_Date": datetime.datetime.strptime(row[5], "%m/%d/%Y").strftime("%Y-%m-%d"), "Order_ID": row[6], "Ship_Date":  datetime.datetime.strptime(row[7], "%m/%d/%Y").strftime("%Y-%m-%d"), "Units_Sold": row[8], "Unit_Price": row[9],  "Unit_Cost": row[10], "Total_Revenue": row[11], "Total_Cost": row[12], "Total_Profit": row[13]} 
               for row in csv_reader]
      )

#loaddata()

query_list = ["select count(*) from sales.hrtable where Region='Asia'", "select count(*) from sales.hrtable where Item_Type='Snacks'","select Country, Region, max(Units_Sold) from sales.hrtable group by Country, Region having Region='Europe' limit 1","select Region, max(Units_Sold) from sales.hrtable group by Region limit 1","select Region, max(Total_Revenue) from sales.hrtable group by Region limit 1","select count(*) from sales.hrtable where Region='Europe'"]
print(datetime.now())
for query_text in query_list:
   sql_query = text(query_text)
   result = engine.connect().execute(sql_query)
   result_as_list = result.fetchall()

   for row in result_as_list:
      print(row)

query_file = open("D:\TA_Project\queries.sql")
query_data = query_file.read()
query_file.close()
queries_in_file = query_data.split('\n')
print(queries_in_file)
print("Queries from File")
for query_text in queries_in_file:
   sql_query = text(query_text)
   result = engine.connect().execute(sql_query)
   result_as_list = result.fetchall()

   for row in result_as_list:
      print(row)

class Solution(Base):
    _tablename_ = 'solution'
    #_table_args_ = {'extend_existing': True}
    #ID=Column(Integer,primary_key=True, autoincrement=True)
    Run_ID = Column(Integer,primary_key=True)
    Query_ID=  Column(String(255))
    Query_Text =  Column(String(255))
    Concurrency =  Column(Integer)
    Start_Time = Column(Time)
    End_Time = Column(Time)
    Time_Elapsed =  Column(Integer)
    Query_Status =  Column(String(255))
    Total_Time_In_That_Run = Column(Integer)

def create_table(Base):
   Base.metadata.create_all(engine)

print("Creating Table")
create_table(Base)

def execute_query(queryText, runId, queryId, concurrency, startTime):
   Session = sessionmaker(bind=engine)
   session = Session()
   queryResult = session.execute_query(query_text)
   endTime = time()
   totalTime = endTime - startTime
   solution = Solution(**{
      'Run_ID': runId,
      'Query_ID': queryId,
      'Query_Text': queryText,
      'Concurrency': concurrency,
      'Start_Time': strftime("%H:%M:%S", gmtime(startTime)),
      'End_Time': strftime("%H:%M:%S", gmtime(endTime)),
      'Time_Elapsed': totalTime,
      'Query_Status': "SUCCESS" if queryResult.rowcount > 0 else "FAILED",
   })
   session.add(queryResult)
   session.commit()
   session.close()

queryList = []
def execute_all_queries():
   sqlFile = open('D:\TA_Project\queries.sql', 'r')
   for query in sqlFile:
      queryList.append(query.strip('\n'))
   concurrency = int(3)
   runId = int(1)
   queryId = 20
   startTime = time()
   for i in range(0, len(queryList), concurrency):
      conQueries = queryList[i:i + concurrency]
      conTreads = []
      for query in conQueries:
         t = Thread(target=execute_query, args=(query, runId, queryId, concurrency, time()))
         t.start()
         conTreads.append(t)
         runId = runId + 1
         queryId += 1
      _ = [j.join() for j in con_treads]
   endTime = time()
   totalTime = endTime - startTime
   Session = sessionmaker(bind=engine)
   session = Session()
   session.query(Solution).filter(Solution.Run_ID == runId).update({'Total_Time_In_That_Run': total_time})
   session.commit()
   session.close()

execute_all_queries()
print(datetime.now())
print(os.getenv("CONCURRENCY"))