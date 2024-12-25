'''
Script to parse all syslog files saved by IoTitan and convert them to csv for visualization.
Example usage: python ../IoTitan/batch_analysis.py | sort -n > iotitan_20241225_data_sorted.csv
'''
import os
import re
import gzip
from datetime import datetime

def get_year_from_directory(directory):
    try:
        year = int(directory[:4])
        if 1900 <= year <= datetime.now().year:
            return year
    except ValueError:
        pass
    return datetime.now().year

# 1. Find all files in the current or subdirectories starting with "syslog" and store in found_files
found_files = []
for root, dirs, files in os.walk("."):
    for file in files:
        if file.startswith("syslog"):
            found_files.append(os.path.join(root, file))

# 2. For each file in the found_files, open it, and parse each line as per the given rules
for file_path in found_files:
    # Extract the year from the directory name
    directory_name = os.path.basename(os.path.dirname(file_path))
    year = get_year_from_directory(directory_name)

    # Open the file with appropriate decompression if it ends with ".gz"
    if file_path.endswith(".gz"):
        open_func = gzip.open
        mode = 'rt'  # Read text mode for gzip
    else:
        open_func = open
        mode = 'r'  # Read text mode for regular files

    with open_func(file_path, mode) as file:
        print(f"File={file_path}")
        for line in file:
            # rule1: only process lines containing the string "mqtt_logger", ignore all other lines
            if "mqtt_logger" not in line:
                continue

            # rule2: match the timestamp at the start of the line with format "Dec 25 05:09:04"
            timestamp_match = re.match(r"^[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2}", line)
            if not timestamp_match:
                print(f"#ERROR: invalid timestamp in line {line}")
                continue
            timestamp_str = timestamp_match.group(0)

            # Convert timestamp to Unix time format using the extracted or current year
            try:
                timestamp_dt = datetime.strptime(timestamp_str, "%b %d %H:%M:%S")
            except ValueError:
                print(f"#ERROR: invalid timestamp for conversion to unixtime in line {line}")
                continue
            timestamp_dt = timestamp_dt.replace(year=year)
            timestamp_unix = int(timestamp_dt.timestamp())

            # rule3: match the name of the sensor which is the string starting with "iotitan/home/"
            sensor_name_match = re.search(r"iotitan/home/\S+", line)
            if not sensor_name_match:
                print(f"#ERROR: invalid sensor name in line {line}")
                continue
            sensor_name = sensor_name_match.group(0)

            # rule4: match the value of the sensor which is the number following the sensor name
            sensor_value_match = re.search(r"iotitan/home/\S+ (.*)$", line)
            if not sensor_value_match:
                print(f"#ERROR: invalid sensor value in line {line}")
                continue
            sensor_value = sensor_value_match.group(1)

            # rule5: print the following variables in csv format "timestamp","sensor_name", "sensor_value"
            print(f"{timestamp_unix},{sensor_name},{sensor_value}")
