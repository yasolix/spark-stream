#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import dateutil.parser as ps

def parseDatetime(dt):
    return ps.parse(dt)

def datetostring(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def stringtodate(str):
    return datetime.strptime(str,"%Y-%m-%d %H:%M:%S")

#Cassandra connect
ap = PlainTextAuthProvider(username='cassandra', password='cassandra')
#c = Cluster(contact_points = ['127.0.0.1'],protocol_version=3, auth_provider=ap)
c = Cluster(contact_points = ['10.1.57.41', '10.1.57.40', '10.1.57.38'],protocol_version=3, auth_provider=ap)
#s = c.connect('traffikeyspace')
session = c.connect()
session.set_keyspace('traffickeyspace')
#session.execute("TRUNCATE TABLE Total_Traffic")
#session.execute("TRUNCATE TABLE Window_Traffic")
#session.execute("TRUNCATE TABLE Poi_Traffic")


def insert2DB(message):
    fields = message.value.split(',')
    currentTime = datetime.today()
    bucket = currentTime.strftime("%Y-%m-%d %H:%M:%S")
    timefield = parseDatetime(fields[5])
    print fields[0],fields[1],fields[2],fields[3],fields[4],fields[5], timefield, bucket
    session.execute("INSERT INTO total_traffic (routeid, vehicleType, totalCount, timeStamp, recordDate) VALUES (%s, %s, %s,%s,%s)",
                    (fields[2],fields[1],14,timefield,bucket))
    session.execute("INSERT INTO window_traffic (routeid, vehicleType, totalCount, timeStamp, recordDate) VALUES (%s, %s, %s,%s,%s)",
                    (fields[2],fields[1],15,timefield,bucket))
    session.execute("INSERT INTO poi_traffic (vehicleid, distance,timestamp,vehicletype) VALUES (%s, %s, %s, %s)",
                    (fields[0],2,timefield,fields[1]))


#KAFKA Connection
zookeeper = 'localhost:2181'
brokerlist = ['localhost:9092']
topic = 'traffic-data-event'
group = 'my-group'

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(topic,
                         group_id=group,
                         bootstrap_servers=brokerlist)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

    insert2DB(message)


    #Check for read
    #rows = session.execute('select * from traffickeyspace.poi_traffic')
    #for r in rows:
    #    print r.vehicleid, r.distance, r.timestamp, r.vehicletype

    #t2 = stringtodate(fields[5])
