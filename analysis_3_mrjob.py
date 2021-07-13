#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MRChannelCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw),
            MRStep(mapper=self.mapper_get_pair,
                   reducer=self.reducer_get_unique),
            MRStep(reducer=self.reducer)
        ]

    def mapper_raw(self, path, _):
        """Mapper reads in one row as one list
        and emits rows as values to pass to next mapper
        """

        with open(path, 'r', encoding='utf-8') as file:
            # Read csv file using csv.reader
            reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)

            # Skip header
            next(reader, None)

            # Reading in one row as one list
            for row in reader:
                yield None, row

    def mapper_get_pair(self, _, row):
        """Mapper filters selected countries to study,
        extracts channel and title
        and emits channel & title in pair as key
        """

        # List of country of interest
        country_list = ["Canada", "Great Britain", "United States"]

        # Filter country
        if row[6] in country_list:
            # Extract video title and channel
            title = row[2]
            channel = row[3]
            # Emit channel & title pair as a list
            yield (channel, title), None

    def reducer_get_unique(self, channel_title_pair, _):
        """Reducer sorts channel-title pair,
        reduces them to unique pairs only,
        extracts channel and outputs channel with count of 1
        """
        yield channel_title_pair[0], 1

    def reducer(self, channel, counts):
        """Reducer sums count of channels"""
        yield channel, sum(counts)


if __name__ == '__main__':
    MRChannelCount.run()

