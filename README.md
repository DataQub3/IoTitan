# IoTitan
scalable system for monitoring and activating IoT devices

Current capability is limited to home use.  Weather and motion sensors send data to a MQTT Broker.  Processing agents subscribe to the MQTT broker to calculate statistics and alerts.  Time aggregated values are forwarded to the Cloud for monitoring from anywhere.

## Files

### mqtt_logger.service
Systemd service file to start the MQTT logger script and ensure it runs at all times.
Note: the service file needs copying to /etc/systemd/system/ as root.
This is used for the IoT project described on Google Drive.  
Prerequisite: mosquitto MQTT software must be installed

Commands to control the logger:
sudo systemctl status mqtt_logger
sudo systemctl enable mqtt_logger

The output of the mqtt_logger ends up in syslog.  To view the output run "sudo journalctl -u mqtt_logger"

### mqtt_processor_stats.py
Take a feed of all MQTT messages, calculate statistics, then publish those statistics back to MQTT.
The input feed is taken from the mqtt_logger for simplicity.

### mqtt_processor_alerts.py
Monitors some topics by subscribing to the MQTT broker.  Create alerts for values outside their expected range, and publish those alerts to the MQTT broker.


