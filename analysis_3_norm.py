#!/usr/bin/env python
# coding: utf-8

import csv

# List of country of interest
country_list = ["Canada", "Great Britain", "United States"]
unique_pair = []

# Read csv file
with open('./data/combined_all.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # Skip header
    next(reader, None)

    for row in reader:
        # Filter country
        if row[6] in country_list:
            channel_title_pair = (row[3], row[2])

            # Select unique pair of channel & title
            if channel_title_pair not in unique_pair:
                unique_pair.append(channel_title_pair)

channel = []

# Extract channel
for i in range(len(unique_pair)):
    channel.append(unique_pair[i][0])

# Count occurrence of channel and store in dict
count_dict = {i: channel.count(i) for i in channel}

# Write output to csv file
with open('./output/analysis_3_output.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in count_dict.items():
        writer.writerow([key, value])