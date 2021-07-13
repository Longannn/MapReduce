#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MRCategoryCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw),
            MRStep(mapper=self.mapper_get_category,
                   reducer=self.reducer_count),
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

    def mapper_get_category(self, _, row):
        """Mapper filters selected countries to study,
        extracts category and outputs category with count of 1
        """

        # List of country of interest
        country_list = ["Canada", "Great Britain", "United States"]

        # Filter country
        if row[6] in country_list:
            category = row[5]
            yield category, 1

    def reducer_count(self, category, counts):
        """Reducer sums count of category"""
        yield category, sum(counts)


if __name__ == '__main__':
    MRCategoryCount.run()

