import pymysql
import csv
import datetime
import logging
import os
from dotenv import load_dotenv, set_key
from pathlib import Path
from datetime import datetime


env_path = Path('D:/TA_Project/param.env')
load_dotenv(env_path)
from sqlalchemy import create_engine,Table, Column, Integer, String, MetaData, Float, Date, BigInteger, text, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import *
from time import time
from threading import Thread
from time import *


#Fetching Values from Environment Variables
USER_NAME = os.getenv('USER_NAME')
PASSWORD=os.getenv('PASSWORD')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DRIVER=os.getenv('DRIVER')
CONCURRENCY = os.getenv('CONCURRENCY')
QUERY_DIR=os.getenv('QUERY_DIR')


#Connecting with MySQL
engine = create_engine(DRIVER)
print("Connection Established")
Base = declarative_base()

class HumanResourceData(Base):
    __tablename__ = 'HumanResourceData'
    ID = Column(Integer, primary_key=True)
    Region = Column(String(255))
    Country = Column(String(255))
    ItemType = Column(String(255))
    SalesChannel = Column(String(255))
    OrderPriority = Column(String(255))
    OrderDate = Column(Date)
    OrderID = Column(Integer)
    ShipDate = Column(Date)
    UnitsSold = Column(Integer)
    UnitPrice = Column(Float)
    UnitCost = Column(Float)
    TotalRevenue=Column(Float)
    TotalCost=Column(Float)
    TotalProfit=Column(Float)

class QueryRunTable(Base):
    __tablename__ = 'QueryRunTable'
    RunID = Column(Integer,primary_key=True)
    BlockID = Column(Integer)
    QueryID=  Column(String(255))
    QueryText =  Column(String(255))
    Concurrency =  Column(Integer)
    StartTime = Column(Time)
    EndTime = Column(Time)
    TimeElapsed =  Column(Integer)
    QueryStatus =  Column(String(255))
    TotalTimeInThatRun = Column(String(255))

def create_table(Base):
    Base.metadata.create_all(engine)

create_table(Base)

def loadDataIntoDB():
    Session = sessionmaker(bind=engine)
    session = Session()
    with open('C:\\Users\\Praghadeesh\\Downloads\\5kSalesRec.csv', 'r', encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            temp_data = HumanResourceData(Region=row[0],Country=row[1],ItemType= row[2],SalesChannel=row[3], OrderPriority=row[4], OrderDate= datetime.datetime.strptime(row[5], "%m/%d/%Y").strftime("%Y-%m-%d"), OrderID=row[6], ShipDate= datetime.datetime.strptime(row[7], "%m/%d/%Y").strftime("%Y-%m-%d"), UnitsSold= row[8], UnitPrice= row[9],  UnitCost= row[10], TotalRevenue= row[11], TotalCost= row[12], TotalProfit= row[13])
            session.add(temp_data)
            session.commit()
            session.close()
      

#Uncomment loadDataIntoDB to load the data into Database
# loadDataIntoDB()
def executeQuery(queryText, runId, queryId, concurrency, startTime, blockId):
   Session = sessionmaker(bind=engine)
   session = Session()
   queryResult = session.execute(queryText)
   sleep(2)
   endTime = time()
   totalTime = endTime - startTime
   queryRunTable = QueryRunTable(**{
      'RunID': runId,
      'BlockID' : blockId,
      'QueryID': queryId,
      'QueryText': queryText,
      'Concurrency': concurrency,
      'StartTime': strftime("%H:%M:%S", gmtime(startTime)),
      'EndTime': strftime("%H:%M:%S", gmtime(endTime)),
      'TimeElapsed': totalTime,
      'QueryStatus': "SUCCESS" if queryResult.rowcount > 0 else "FAILED",
   })
   session.add(queryRunTable)
   session.commit()
   session.close()

queryList = []
def executeAllQueries():
   sqlFile = open(QUERY_DIR, 'r')
   for query in sqlFile:
      queryList.append(query.strip('\n'))
   concurrency = int(CONCURRENCY)
   runId = int(1)
   queryId = 20
   blockId = int(1)
   
   for i in range(0, len(queryList), concurrency):
      conQueries = queryList[i:i + concurrency]
      conTreads = []
      startTime = time()
      for query in conQueries:
         thread = Thread(target=executeQuery, args=(query, runId, queryId, concurrency, time(), blockId))
         thread.start()
         conTreads.append(thread)
         runId = runId + 1
         queryId += 1
      
      blockId += 1
      endTime = time()
      totalTime = endTime - startTime
      Session = sessionmaker(bind=engine)
      session = Session()
      # print("Total time " + str(totalTime))
      # print("Block ID " + str(blockId))
      # session.query(QueryRunTable).filter(QueryRunTable.RunID == runId).update({'TotalTimeInThatRun': totalTime})
      # print(session.query(QueryRunTable).filter(QueryRunTable.BlockID == blockId))
      # stmt = (update(QueryRunTable).where(QueryRunTable.BlockID == blockId).values(TotalTimeInThatRun='user #5'))
      # print("Statement " + str(stmt))
      # session.execute("UPDATE QueryRunTable SET TotalTimeInThatRun = 'some' WHERE BlockID = 1")
      statement = session.query(QueryRunTable).filter(QueryRunTable.BlockID == blockId).update({'TotalTimeInThatRun': str(totalTime)})
      session.commit()
      session.close()
      set_key('param.env', key_to_set='RUN_ID', value_to_set=str(runId))

executeAllQueries()

print("Data Loaded into Query Result Table")
