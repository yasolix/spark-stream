#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys

import os
import sys


# Path for spark source folder
#os.environ['SPARK_HOME'] = "/home/ozlem/Downloads/spark-2.0.0-bin-hadoop2.4"
#Line above should be adjusted according to local spark folder
os.environ['SPARK_HOME'] = "/home/yt/spark"
#os.environ['PYSPARK_SUBMIT_ARGS'] = ( "--conf spark.cassandra.connection.host=10.1.57.41" )
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--conf spark.cassandra.connection.host=10.1.57.40 pyspark-shell"
)
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.1-s_2.11'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 '


try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    from pyspark.mllib.clustering import KMeans

    from pyspark.sql import Row, DataFrame, SQLContext, SparkSession
    from pyspark.sql.functions import array
    from pyspark.sql.types import array,FloatType,TimestampType,DataType
    from pyspark.ml.feature import VectorAssembler

    import dateutil.parser as ps
    from datetime import datetime
    from math import sqrt


except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def parseDatetime(dt):
    return ps.parse(dt)

def dateToUxtime(dt):
    return dt.strftime('%s')

if __name__ == "__main__":

    #Spark context
    sc = SparkContext('local[2]')
    ssc = StreamingContext(sc, 5)
    sqlContext = SQLContext(sc)


    #Kafka connection
    #zkQuorum, topic = sys.argv[1:]
    zkQuorum = 'localhost:2181'
    topic = 'traffic-data-event'
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])

    projection = lines.map(lambda line: line.replace('"','').split(',')).map(lambda entry:
                                         Row(vehicleid=entry[0],
                                               type=entry[1],
                                               route=entry[2],
                                               lat=float(entry[3]),
                                               lon=float(entry[4]),
                                               zaman=parseDatetime(entry[5])
                                               ))

    lines.pprint()
    projection.pprint()

# Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s Process =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())
            print("========= %s Session =========" % str(time))

            readtableDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="test_traffic", keyspace="traffickeyspace").load().show()

            print(type(readtableDF))

            # Convert RDD[Row] to DataFrame
            rddDF = spark.createDataFrame(rdd)
            rddDF.printSchema()
            rddDF.show()

            # Write Dataframe to Cassandra
            rddDF.select('vehicleid','lat','lon','route','zaman','type').write.format("org.apache.spark.sql.cassandra").mode('append').options(table="log_traffic", keyspace="traffickeyspace").save()
            rddDF.select('vehicleid','lat','zaman',).write.format("org.apache.spark.sql.cassandra").mode('append').options(table="test_traffic", keyspace="traffickeyspace").save()


            #rddDF.createOrReplaceTempView("TrafficTempTbl")

            print("========= %s Dataframe is created =========" % str(time))

        except:
           pass


    projection.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()



    '''
    #os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-s_2.11 '
    #from cassandra.auth import PlainTextAuthProvider
    #from cassandra.cluster import Cluster
    #Cassandra connection
    ap = PlainTextAuthProvider(username='cassandra', password='cassandra')
    c = Cluster(contact_points = ['10.1.57.41', '10.1.57.40', '10.1.57.38'],protocol_version=3, auth_provider=ap)
    session = c.connect()
    session.set_keyspace('traffickeyspace')

    #Truncate tables
    #session.execute("TRUNCATE TABLE Total_Traffic")
    #session.execute("TRUNCATE TABLE Window_Traffic")
    #session.execute("TRUNCATE TABLE Poi_Traffic")


    def insert2DB(message):
        fields = message.split(',')
        currenttime = datetime.today()
        bucket = currenttime.strftime("%Y-%m-%d %H:%M:%S")
        timefield = parseDatetime(fields[5])

        session.execute("INSERT INTO total_traffic (routeid, vehicleType, totalCount, timeStamp, recordDate) VALUES (%s, %s, %s,%s,%s)",
                        (fields[2],fields[1],14,timefield,bucket))
        session.execute("INSERT INTO window_traffic (routeid, vehicleType, totalCount, timeStamp, recordDate) VALUES (%s, %s, %s,%s,%s)",
                        (fields[2],fields[1],15,timefield,bucket))
        session.execute("INSERT INTO poi_traffic (vehicleid, distance,timestamp,vehicletype) VALUES (%s, %s, %s, %s)",
                        (fields[0],2,timefield,fields[1]))

    ap = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(contact_points=['10.1.57.41', '10.1.57.40','10.1.57.38'],protocol_version=3, auth_provider=ap)
    session = cluster.connect()
    session.set_keyspace('traffickeyspace')
    prepared = session.prepare("INSERT INTO total_traffic (routeid, vehicleType, totalCount, timeStamp, recordDate) VALUES (?,?,?,?,?)")

    def savetodb(rt,typ,cnt,tt,rec):
        session.execute(prepared.bind(rt,typ,cnt,tt,rec))

    entries = kvs.map(lambda x: x[1]).map(lambda line: line.replace('"','').split(','))
    entries.map(lambda entry : savetodb(entry[0],entry[2],entry[1],200,parseDatetime(entry[5]),'rec'))
    '''
    '''
    c = Cluster(contact_points = ['10.1.57.41', '10.1.57.40', '10.1.57.38'],protocol_version=3, auth_provider=ap)
    s = c.connect('genelks')
    rows = s.execute('select sid, adi, dogum_tarihi, soyad from t1')
    for user_row in rows:
        print(user_row.sid, user_row.adi,user_row.soyad, user_row.dogum_tarihi)
    '''