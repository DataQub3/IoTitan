# MQTT STATISTICS PROCESSOR
# - Subscribe to MQTT Broker
# - Process statistics from data
# - Publish statistics back to same MQTT Broker
# TODO: currently only supports plaintext.  See https://mntolia.com/mqtt-python-with-paho-mqtt-client/ for single() and multiple()
# To run this script, need to feed it all MQTT topics from past "publish_interval" seconds
# i.e. journalctl -u mqtt_logger --since="2019-04-11 23:08:15"  | python3 mqtt_processor_stats.py

from __future__ import print_function
# Main methods of the paho mqtt library are: publish, subscribe, unsubscribe, connect, disconnect
import paho.mqtt.client as mqtt
import time
import subprocess
import pandas as pd
import sys

###   Start of user configuration   ###
# only publish upstream periodically, e.g. every 5 minutes
publish_interval = 60
start_time = "2019-04-11 14:08:15"
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

def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))

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

# Run a loop which reads the systemd journal every "publish_interval" seconds.
# Calculated statistics of subscribed topics from the journal.  Note: an mqtt_logger saves
# messages from all topics to the journal.
# Lastly, publish the results to a stats channel using MQTT
client = mqtt.Client()
client.on_log=on_log
client.on_connect = on_connect

print("Connecting to broker")
# params are: hostname, port, keepalive, bind_address
client.connect(mqtt_host, tPort, 60)

# Read the journal from stdin
df = pd.read_csv(sys.stdin, header=None, index_col=0)
df.columns = ['time', 'host', 'topic', 'value']
print(df.mean())
print(df)
# publish statistics
#current = publish_statistics(client, msg, current, publish_interval)

