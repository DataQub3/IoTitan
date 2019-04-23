# MQTT BRIDGE TO THINGSPEAK
# - Subscribe to local MQTT Broker containing data from local sensors
# - Filter data to the subset to send to ThingSpeak Cloud service
# - Publish filtered data to our ThingSpeak channel

from __future__ import print_function
import sys
# Use Paho MQTT library to connect to local MQTT broker
# Main methods of the paho mqtt library are:
# - publish, subscribe, unsubscribe, connect, disconnect
import paho.mqtt.client as mqtt
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import json

# stderr print
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

lastThingspeakTime = time.time()
thingspeakInterval = 15  # post to Thingspeak minimum time interval
thingspeakTimeout = 610  # more than 10 minutes

# ----------  Start of user configuration ----------
# ThingSpeak Channel Settings
# The ThingSpeak Channel ID
channelID = "YOUR THINGSPEAK CHANNEL ID"
# The Write API Key for the channel
writeApiKey = "YOUR THINGSPEAK WRITE API KEY"
url = "https://api.thingspeak.com/channels/" + channelID + "/bulk_update.json"
#url = "http://httpbin.org/post"
fields_ts = dict()  # global dict to hold current values of all fields for thingspeak

# Hostname of the MQTT service
mqtt_host = "127.0.0.1"  # customise as required
tPort = 0
# MQTT Connection Methods
# use default MQTT port 1883 (low system cost)
use_unsecured_TCP = True
# use unsecured websocket on port 80 (useful when 1883 blocked)
use_unsecured_websockets = False
# use secure websocket on port 443 (most secure)
use_SSL_websockets = False
# ---------- End of user configuration ----------

def http_request():
    # Function to send the POST request to ThingSpeak channel for bulk update.
    global fields_ts
    # Need to send a timestamp or relative time with the readings
    fields_ts['delta_t'] = int(round(time.time() - lastThingspeakTime))
    message_buffer = []  # empty list
    message_buffer.append(fields_ts)
    #for k,v in sorted(fields_ts.items()):
    #    if k == 'field1':
    #        messageBuffer.append({k: v})
    data_dict = {'write_api_key': writeApiKey, 'updates': message_buffer}
    # Format json data as string rather than Python dict, then byte encode.
    json_data = json.dumps(data_dict).encode('utf-8')
    eprint("data: %s" % (json_data, ))
    request_headers = {"User-Agent": "mw.doc.bulk-update (Raspberry Pi)", \
                       "Content-Type": "application/json", \
                       "Content-Length": str(len(json_data))}
    req = Request(url=url, data=json_data, headers=request_headers, method='POST')
    eprint("sending URL request to ThingSpeak")
    try:
        response = urlopen(req) # Make the request
        eprint(response.read().decode())
        eprint(response.getcode())  # A 202 indicates success
    except Exception as inst:
        eprint(type(inst))  # the exception instance
        eprint(inst.args)  # arguments stored in .args
        eprint(inst)  # __str__ allows args to be printed directly
        pass
    fields_ts.clear()  # clear the fields_ts dict after sending


def update_thingspeak_rest_api():
    # Function to update the message buffer with sensor readings
    # and then call the http_request function if we are ready to update ThingSpeak.
    # This (used to) use the relative timestamp as it uses the "delta_t" param
    global lastThingspeakTime
    global thingspeakInterval
    global thingspeakTimeout
    global fields_ts
    # check whether all fields are ready to be sent
    #if (all fields exist AND thingspeakInterval elapsed):
    if ('field1' in fields_ts and 'field2' in fields_ts and 'field3' in fields_ts and 'field4' in fields_ts and 'field5' in fields_ts) and ((time.time() - lastThingspeakTime) >= thingspeakInterval):
        http_request()
        lastThingspeakTime = time.time()
    elif ('field1' in fields_ts or 'field2' in fields_ts or 'field3' in fields_ts or 'field4' in fields_ts or 'field5' in fields_ts) and ((time.time() - lastThingspeakTime) >= thingspeakTimeout):
        http_request()
        lastThingspeakTime = time.time()

# Set up the connection parameters based on the connection type
if use_unsecured_TCP:
    tTransport = "tcp"
    tPort = 1883
    tTLS = None

if use_unsecured_websockets:
    tTransport = "websockets"
    tPort = 80
    tTLS = None

if use_SSL_websockets:
    import ssl
    tTransport = "websockets"
    tTLS = {'ca_certs': "/etc/ssl/certs/ca-certificates.crt", \
            'tls_version': ssl.PROTOCOL_TLSv1}
    tPort = 443


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    eprint("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # For multiple subscriptions, put them in a list of tuples
    client.subscribe([("iotitan/home/up_bed1/dht11/temperature/average", 0), \
                      ("iotitan/home/up_bed1/dht11/humidity/average", 0), \
                      ("iotitan/home/up_bed1/xc-4444/pir/average", 0), \
                      ("iotitan/home/up_bed4/dht11/temperature/average", 0), \
                      ("iotitan/home/up_bed4/dht11/humidity/average", 0)])  # qos=0


def on_disconnect(client, userdata, rc):
    eprint("disconnecting reason " + str(rc))


def on_log(client, userdata, level, buf):
    # print("on_log")
    eprint("log: %s" % (buf, ))


# The callback for when a PUBLISH message is received
# from the server that matches our subscription.
# Note: msg is of message class with members: topic, qos, payload, retain
def on_message(client, userdata, msg):
    global fields_ts

    if msg.topic == "iotitan/home/up_bed1/dht11/temperature/average":
        fields_ts['field1'] = float(msg.payload.decode("utf-8"))
    elif msg.topic == "iotitan/home/up_bed1/dht11/humidity/average":
        fields_ts['field2'] = float(msg.payload.decode("utf-8"))
    elif msg.topic == "iotitan/home/up_bed1/xc-4444/pir/average":
        fields_ts['field3'] = float(msg.payload.decode("utf-8"))
    elif msg.topic == "iotitan/home/up_bed4/dht11/temperature/average":
        fields_ts['field4'] = float(msg.payload.decode("utf-8"))
    elif msg.topic == "iotitan/home/up_bed4/dht11/humidity/average":
        fields_ts['field5'] = float(msg.payload.decode("utf-8"))
    #sensor_reading = float(msg.payload.decode("utf-8"))
    #print("Sending data: field1 = %f" % (sensor_reading, ))
    # Could send this message to ThinkSpeak using MQTT
    # but I wasn't clear how to do a MQTT loop on two bridged servers
    # client_ts = mqtt.Client()
    # client_ts.connect("mqtt.thingspeak.com", 1883, 60)
    # client_ts.publish("channels/%s/publish/%s" % (channelId,apiKey),
    # "field1=" + sensor_reading)
    # send message to ThingSpeak using REST API (https post)
    update_thingspeak_rest_api()

# ////////////////////////////////////////////
# Start of main
# ////////////////////////////////////////////
client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_log = on_log

# eprint("Connecting to local MQTT broker")
# params are: hostname, port, keepalive, bind_address
client.connect(mqtt_host, tPort, 60)

# eprint("Subscribing to channels")
# client.subscribe([("$SYS/#",0),("/#",0)]) #format for multiple subscriptions
client.subscribe([("iotitan/home/up_bed1/dht11/temperature/average", 0), \
        ("iotitan/home/up_bed1/dht11/humidity/average", 0), \
        ("iotitan/home/up_bed1/xc-4444/pir", 0), \
        ("iotitan/home/up_bed4/dht11/temperature/average", 0), \
        ("iotitan/home/up_bed4/dht11/humidity/average", 0)])  # qos=0


# eprint("Looping for callbacks")
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
eprint("End of loop")
