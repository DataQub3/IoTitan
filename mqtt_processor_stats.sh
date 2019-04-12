#!/bin/bash
/bin/journalctl -u mqtt_logger --since="`/bin/date -d "-5 min" +"%Y-%m-%d %H:%M:%S"`" | grep -v "^--" | grep -v "average" | cut -d ":" -f 4 | perl -nle 's/^\s//g; s/\s/, /g; print $_' | /usr/bin/python3 /home/pi/IoTitan/mqtt_processor_stats.py
