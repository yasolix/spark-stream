from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import time
import random
import uuid

def getCoordinates(routeId):
    if (routeId == "Route-37"):
        latprefix = 33
        lonprefix = -96
    elif (routeId == "Route-82"):
        latprefix = 34
        lonprefix = -97
    elif (routeId == "Route-43"):
        latprefix = 35
        lonprefix = -98

    lat = str(latprefix + random.random())
    lon = str(lonprefix + random.random())
    return lat,lon

zookeeper = 'localhost:2181'
brokerlist = ['localhost:9092']
topic = 'traffic-data-event'

path = "/home/yt/datascience/uber-tlc-foil-response/uber-trip-data/"
filename = path + "uber-raw-data-apr14.csv"


#producer = KafkaProducer(bootstrap_servers=['broker1:1234'])
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers=brokerlist)

routeList = ["Route-37", "Route-43", "Route-82"]
vehicleTypeList = ["Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"]


for i in range(100):

    vehicleId = uuid.uuid4()
    vehicleType = random.choice(vehicleTypeList)
    routeId = random.choice(routeList)
    speed = random.randint(20,100)
    fuelLevel = random.randint(10,40)

    lat,lon = getCoordinates(routeId)
    # Asynchronous by default
    vehiclestring = str(vehicleId) +','+ str(vehicleType)+','+ str(routeId)+','+lat +','+ lon
    #future = producer.send(topic, vehiclestring + ',' + str(datetime.now()))
    future = producer.send(topic, vehiclestring + ',' + str(datetime.now()))

    print vehiclestring + ',' + str(datetime.now())

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass

    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)

    time.sleep(1)


'''
# produce asynchronously
for i in range(100):
    producer.send('test', b'msg')

# produce keyed messages to enable hashed partitioning
producer.send('my-topic', key=b'foo', value=b'bar')

# encode objects via msgpack
producer = KafkaProducer(value_serializer=msgpack.dumps)
producer.send('msgpack-topic', {'key': 'value'})

# produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer.send('json-topic', {'key': 'value'})

# produce asynchronously
for _ in range(100):
    producer.send('my-topic', b'msg')

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
'''