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
thingspeakInterval = 1  # post date to Thingspeak at this interval

# ////////   Start of user configuration ////////
# ThingSpeak Channel Settings
# The ThingSpeak Channel ID
channelID = "752701"
# The Write API Key for the channel
writeApiKey = "CW2MNKS3DR9GXP2X"
url = "https://api.thingspeak.com/channels/" + channelID + "/bulk_update.json"
#url = "http://httpbin.org/post"
messageBuffer = []

# Hostname of the MQTT service
mqtt_host = "192.168.169.233"
tPort = 0
# MQTT Connection Methods
# use default MQTT port 1883 (low system cost)
use_unsecured_TCP = True
# use unsecured websocket on port 80 (useful when 1883 blocked)
use_unsecured_websockets = False
# use secure websocket on port 443 (most secure)
use_SSL_websockets = False
# ///////   End of user configuration ////////

def http_request():
    # Function to send the POST request to ThingSpeak channel for bulk update.
    global messageBuffer
    data_dict = {'write_api_key': writeApiKey, 'updates': messageBuffer}
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
    messageBuffer = []  # Reinitialize the message buffer


def update_thingspeak_rest_api(temperature, humidity):
    # Function to update the message buffer with sensor readings
    # and then call the http_request function every 2 minutes.
    # This examples uses the relative timestamp as it uses the "delta_t" param
    global lastThingspeakTime
    global thingspeakInterval
    message = {}
    message['delta_t'] = int(round(time.time() - lastThingspeakTime))
    if temperature > 0:
        message['field1'] = temperature
    if humidity > 0:
        message['field2'] = humidity
    global messageBuffer
    messageBuffer.append(message)
    # update ThingSpeak channel if suitable time interval
    eprint("time since last update = %i" % (time.time() - lastThingspeakTime))
    eprint("need to wait until %i" % thingspeakInterval)
    if (time.time() - lastThingspeakTime) >= thingspeakInterval:
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
    client.subscribe([("/weatherj/TempAndHumid/Temperature/average", 0), \
                      ("/weatherj/TempAndHumid/Humidity/average", 0)])  # qos=0


def on_disconnect(client, userdata, rc):
    eprint("disconnecting reason " + str(rc))


def on_log(client, userdata, level, buf):
    # print("on_log")
    eprint("log: %s" % (buf, ))


# The callback for when a PUBLISH message is received
# from the server that matches our subscription.
# Note: msg is of message class with members: topic, qos, payload, retain
def on_message(client, userdata, msg):
    temperature_r = -1.0  # initialise to invalid reading
    humidity_r = -1.0
    if msg.topic == "/weatherj/TempAndHumid/Temperature/average":
        temperature_r = float(msg.payload.decode("utf-8"))
    elif msg.topic == "/weatherj/TempAndHumid/Humidity/average":
        humidity_r = float(msg.payload.decode("utf-8"))
    #sensor_reading = float(msg.payload.decode("utf-8"))
    #print("Sending data: field1 = %f" % (sensor_reading, ))
    # Could send this message to ThinkSpeak using MQTT
    # but I wasn't clear how to do a MQTT loop on two bridged servers
    # client_ts = mqtt.Client()
    # client_ts.connect("mqtt.thingspeak.com", 1883, 60)
    # client_ts.publish("channels/%s/publish/%s" % (channelId,apiKey),
    # "field1=" + sensor_reading)
    # send message to ThingSpeak using REST API (https post)
    update_thingspeak_rest_api(temperature_r, humidity_r)
    eprint("Posted equivalent of: " + msg.topic + " " + msg.payload.decode("utf-8"))
    # time.sleep(15) # Thingspeak requires at least 15 seconds between updates


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
client.subscribe([("/weatherj/TempAndHumid/Temperature/average", 0), \
                  ("/weatherj/TempAndHumid/Humidity/average", 0)])  # qos=0


# eprint("Looping for callbacks")
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
eprint("End of loop")
