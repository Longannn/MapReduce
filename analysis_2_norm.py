#!/usr/bin/env python
# coding: utf-8

import csv

# List of country of interest
country_list = ["Canada", "Great Britain", "United States"]
count_dict = {}

# Read csv file
with open('./data/combined_all.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)

    for row in reader:
        # Filter country
        if row[6] in country_list:
            # Extract category
            category = row[5]

            if category in count_dict.keys():
                count_dict[category] += 1

            else:
                count_dict[category] = 1

# Write output to csv file
with open('./output/analysis_2_output.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in count_dict.items():
        writer.writerow([key, value])