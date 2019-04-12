# MQTT ALERTS PROCESSOR
# - Subscribe to MQTT Broker
# - Process incoming data looking for anomalies
# - Publish alerts back to same MQTT Broker
# TODO: currently only supports plaintext.  See https://mntolia.com/mqtt-python-with-paho-mqtt-client/ for single() and multiple()

from __future__ import print_function
# Main methods of the paho mqtt library are: publish, subscribe, unsubscribe, connect, disconnect
import paho.mqtt.client as mqtt
import time

###   Start of user configuration   ###
# only publish upstream periodically, e.g. every 5 minutes
publish_interval = 60
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


previous = time.time() # timestamps used to decide when to publish statistics
current = time.time()

def on_log(client, userdata, level, buf):
    print("log: ". buf)

def publish_statistics(client, msg, previous_time, interval):
    #print("testing publish_statistics")
    # use a timer to only publish once every X seconds
    current = time.time()
    if (current - previous_time > interval):
        # time to publish statistics
        print("Publish statistics")
        # TODO fix next line. For now just print any message
        client.publish(topic = "weatherj/stats", payload = msg.payload)
        return current
    return previous_time

def publish_alert(client, msg):
    print("publish alert")
    client.publish(topic = msg.topic + "/alert", payload = msg.payload)

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

# Create the topic string
#topic = "channels/" + channelID + "/publish/" + apiKey

# Run a loop which calculates statistics of subscribed topics every
#   5 minutes and publishes the results to a stats channel using MQTT

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # For multiple subscriptions, put them in a list of tuples
    client.subscribe("/#",0)

# The callback for when a PUBLISH message is received from the server.
# Note: msg is of message class with members: topic, qos, payload, retain
def on_message(client, userdata, msg):
    #global current
    #global publish_interval
    print(msg.topic+" "+str(msg.payload))
    # Add the topic and value to a hash    
    # publish statistics
    #current = publish_statistics(client, msg, current, publish_interval)
    if (msg.payload > 50):
        publish_alert(client. msg)

client = mqtt.Client()
client.on_log=on_log
client.on_connect = on_connect
client.on_message = on_message

print("Connecting to broker")
# params are: hostname, port, keepalive, bind_address
client.connect(mqtt_host, tPort, 60)

print("Subscribing to channels")
#client.subscribe([("$SYS/#",0),("/#",0)]) #format for multiple subscriptions
client.subscribe("/#",0)

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
