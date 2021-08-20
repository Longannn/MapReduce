#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MRCategoryCount(MRJob):

    def mapper(self, _, line):
        """Mapper reads in one row as one list
        and emits country & category in pair as key
        """
        # Read csv file using csv.reader
        file = csv.reader([line], quoting=csv.QUOTE_ALL, skipinitialspace=True)

        # Reading in one row as one list
        for row in file:
            # Emit country & category pair as a list
            yield (row[4], row[3]), 1

    def reducer(self, country_category_pair, counts):
        """Reducer sums count of category by country"""

        # Divide sum by 10 to reflect the real count
        yield country_category_pair, sum(counts)/10


if __name__ == '__main__':
    MRCategoryCount.run()
