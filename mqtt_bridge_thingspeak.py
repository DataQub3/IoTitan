# MQTT BRIDGE TO THINGSPEAK 
# - Subscribe to local MQTT Broker containing data from local sensors
# - Filter data to the subset we want to send to ThingSpeak Cloud service for monitoring from anywhere
# - Publish filtered data to our ThingSpeak channel

from __future__ import print_function
# Use Paho MQTT library to connect to local MQTT broker
# Main methods of the paho mqtt library are: publish, subscribe, unsubscribe, connect, disconnect
import paho.mqtt.client as mqtt
import time
import urllib as ul
import json

lastThingspeakTime = time.time()
thingspeakInterval = 30  #post date to Thingspeak at this interval

###   Start of user configuration   ###
#  ThingSpeak Channel Settings
# The ThingSpeak Channel ID
channelID = "752701"
# The Write API Key for the channel
writeApiKey = "CW2MNKS3DR9GXP2X"
url = "https://api.thingspak.com/channels/" + channelID + "/bulk_update.json"
messageBuffer = []

def httpRequest():
    #Function to send the POST request to ThingSpeak channel for bulk update.
    print("in httpRequest")
    global messageBuffer
    data = json.dumps({'write_api_key':writeAPIkey,'updates':messageBuffer}) # Format the json data buffer
    req = ul.Request(url = url)
    aasdf
    requestHeaders = {"User-Agent":"mqtt_broker_thingspeak","Content-Type":"application/json","Content-Length":str(len(data))}
    for key,val in requestHeaders.iteritems(): # Set the headers
        req.add_header(key,val)
    req.add_data(data) # Add the data to the request
    # Make the request to ThingSpeak
    try:
        response = ul.urlopen(req) # Make the request
        print(response.getcode()) # A 202 indicates that the server has accepted the request
    except ul.HTTPError as e:
        print(e.code) # Print the error code
    messageBuffer = [] # Reinitialize the message buffer
    global lastUpdateTime
    lastUpdateTime = time.time() # Update the connection time

def updatesJson(temperature, humidity):
    #Function to update the message buffer with sensor readings and then call the httpRequest function every 2 minutes.
    #This examples uses the relative timestamp as it uses the "delta_t" parameter. 
    print("In updatesJson")
    global lastThingspeakTime
    print("x")
    message = {}
    print("y")
    message['delta_t'] = int(round(time.time() - lastThingspeakTime))
    if (temperature > 0): message['field1'] = temperature
    if (humidity > 0): message['field2'] = humidity
    global messageBuffer
    messageBuffer.append(message)
    print("testing times")
    # If posting interval time has crossed 2 minutes update the ThingSpeak channel with your data
    if time.time() - lastThingspeakTime >= thingspeakInterval:
        httpRequest()
    lastThingspeakTime = time.time()

# Hostname of the MQTT service
mqtt_host = "localhost"
# MQTT Connection Methods
# use default MQTT port 1883 (low system cost)
use_unsecured_TCP = True
# use unsecured websocket on port 80 (useful when 1883 blocked)
use_unsecured_websockets = False
# use secure websocket on port 443 (most secure)
use_SSL_websockets = False
###   End of user configuration   ###

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
    tTLS = {'ca_certs':"/etc/ssl/certs/ca-certificates.crt",'tls_version':ssl.PROTOCOL_TLSv1}
    tPort = 443



# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # For multiple subscriptions, put them in a list of tuples
    client.subscribe("/weatherj/TempAndHumid/Temperature",0)

def on_disconnect(client, userdata, rc):
    print("disconnecting reason " + str(rc))

def on_log(client, userdata, level, buf):
    print("log: ". buf)

# The callback for when a PUBLISH message is received from the server that matches our subscription.
# Note: msg is of message class with members: topic, qos, payload, retain
def on_message(client, userdata, msg):
    print("hi")
    sensor_reading = float(msg.payload.decode("utf-8"))
    #print("Sending to ThingSpeak channel: channels/%s/publish/%s" % (channelId, apiKey))
    print("Sending data: field1 = %s" % (sensor_reading, ))
    # Send this message to ThinkSpeak using MQTT 
    #client_ts = mqtt.Client()
    #client_ts.connect("mqtt.thingspeak.com", 1883, 60)
    #client_ts.publish("channels/%s/publish/%s" % (channelId,apiKey), "field1=" + sensor_reading)
    # send message to ThingSpeak using REST API (https post)
    updatesJson(sensor_reading, -1.0)
    # break
    print(msg.topic + " " + sensor_reading)
    #time.sleep(15) # Thingspeak requires at least 15 seconds between updates


######################################################
# Start of main
######################################################
client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_log = on_log

print("Connecting to local MQTT broker")
# params are: hostname, port, keepalive, bind_address
client.connect(mqtt_host, tPort, 60)

print("Subscribing to channels")
#client.subscribe([("$SYS/#",0),("/#",0)]) #format for multiple subscriptions
client.subscribe("/weatherj/TempAndHumid/Temperature",0)

#print("Publishing a test topic")
# client.publish() params are: topic, payload, qos, retain flag
#client.publish(topic = "TestTopic", payload = "This is a test message")

print("Looping for callbacks")
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
print("End of loop")
