# MQTT STATISTICS PROCESSOR
# - read data from stdin = all data in MQTT broker in past 5 mins
# - Calculate statistics from data
# - connect to a MQTT Broker
# - Publish statistics to that MQTT Broker
# TODO: currently only supports plaintext.  See https://mntolia.com/mqtt-python-with-paho-mqtt-client/ for single() and multiple()
# To run this script, need to feed it all MQTT topics from past "publish_interval" seconds
# journalctl -u mqtt_logger --since="`date -d "-5 min" +"%Y-%m-%d %H:%M:%S"`" | grep -v "^--" | cut -d ":" -f 4 | perl -nle 's/^\s//g; s/\s/,/g; print $_' | python3 mqtt_processor_stats.py

from __future__ import print_function
# Main methods of the paho mqtt library are: publish, subscribe, unsubscribe, connect, disconnect
import paho.mqtt.client as mqtt
#import time
import subprocess
import pandas as pd
import sys
#import dateutil

###   Start of user configuration   ###
# only publish upstream periodically, e.g. every 5 minutes
publish_interval = 60
#start_time = "2019-04-11 14:08:15"
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


#previous = time.time() # timestamps used to decide when to publish statistics
#current = time.time()

def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))

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
client.on_connect = on_connect

print("Connecting to broker")
# params are: hostname, port, keepalive, bind_address
client.connect(mqtt_host, tPort, 60)

# Read the journal from stdin
df = pd.DataFrame.from_csv(sys.stdin, header=None, index_col=None)
print("start")
print(df)
print(df.columns)
print("end")
df.columns = ['topic', 'value']
#df.columns = ['time', 'host', 'topic', 'value']
# Convert date from string to date times
#df['date'] = df['date'].apply(dateutil.parser.parse, dayfirst=False)

#grouped = df.groupby('topic')['value'].mean().to_frame()
grouped = df.groupby('topic')['value'].describe().unstack()
print(grouped.head())

# publish statistics
# the groupby function makes the topic the index
for index, row in grouped.iterrows():
    #mqtt_publish(row['topic'] + '/average', row['value_mean'])
    pub_topic = index + "/average"
    pub_value = str(row['mean'])
    client.publish(topic = pub_topic, payload = pub_value)


