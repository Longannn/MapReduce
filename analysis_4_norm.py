#!/usr/bin/env python
# coding: utf-8

import csv

count_dict = {}

# Read csv file
with open('./data/combined_filtered_upsampled.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # Skip header
    next(reader, None)

    for row in reader:
        # Extract country & category in pair
        country_category_pair = (row[4], row[3])

        # Add count for the same country & category pair
        if country_category_pair in count_dict.keys():
            count_dict[country_category_pair] += 1

        # Initialize if country & category pair not in count_dict
        else:
            count_dict[country_category_pair] = 1

    for key in count_dict.keys():
        count_dict[key] /= 10

# Write output to csv file
with open('./output/normoutput.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in count_dict.items():
        writer.writerow([key, value])
