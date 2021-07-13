#!/usr/bin/env python
# coding: utf-8

import csv
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw),
            MRStep(mapper=self.mapper_get_title,
                   reducer=self.reducer_get_unique),
            MRStep(mapper=self.mapper_clean_title,
                   reducer=self.reducer_wdc)
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

    def mapper_get_title(self, _, row):
        """Mapper filters selected countries to study,
        extracts and emits title
        """

        # List of country of interest
        country_list = ["Canada", "Great Britain", "United States"]

        # Filter country
        if row[6] in country_list:
            # Extract title
            title = row[2]
            # Emit title
            yield title, None

    def reducer_get_unique(self, title, _):
        """Reducer sorts title,
        reduces to titles only,
        and emits unique titles
        """
        yield title, None
        
    def mapper_clean_title(self, title, _):
        """Mapper tokenizes, clean title
        and emits words in title with count 1
        """

        # Remove leading and trailing whitespace
        title = title.strip()

        # Standardize to lowercase
        lowercase = title.lower()

        # Tokenize title into single words
        splitting = lowercase.split()

        # Remove punctuations and numbers
        words = [w for w in splitting if w.isalpha()]

        # Assign stopwords list to variable stops
        stops = ["it's", "they", "with", "himself", "will", "being", "couldn't", "shouldn't", "through", "just", "i",
                 "more", "very", "my", "its", "who", "her", "below", "t", "ve", "shouldn", "whom", "out", "to",
                 "yourselves", "your", "has", "that'll", "mustn't", "these", "the", "over", "hasn't", "she", "in",
                 "are", "didn", "further", "both", "now", "own", "weren't", "for", "all", "not", "should've", "how",
                 "on", "some", "needn't", "hers", "up", "there", "him", "ll", "under", "themselves", "is", "wasn",
                 "you've", "where", "mustn", "than", "herself", "you're", "m", "those", "doing", "was", "before",
                 "most", "s", "and", "any", "we", "here", "wouldn", "hadn't", "of", "ourselves", "as", "did", "them",
                 "can", "a", "ain", "only", "down", "have", "didn't", "an", "off", "other", "then", "she's", "against",
                 "re", "having", "yours", "same", "ma", "theirs", "by", "his", "while", "such", "what", "mightn't",
                 "during", "this", "each", "yourself", "which", "myself", "hasn", "were", "why", "into", "above",
                 "won't", "haven", "their", "until", "it", "or", "if", "be", "aren't", "after", "about", "d",
                 "couldn", "shan", "o", "doesn't", "haven't", "at", "between", "so", "been", "me", "nor", "won",
                 "don", "isn't", "do", "should", "few", "you'd", "ours", "does", "he", "no", "you", "shan't", "once",
                 "wouldn't", "wasn't", "don't", "our", "am", "but", "because", "you'll", "from", "doesn", "isn",
                 "too", "weren", "needn", "y", "itself", "when", "aren", "again", "had", "hadn", "mightn", "that"]

        # Remove stopwords
        words1 = [w for w in words if not w in stops]

        # Remove words with less than 2 characters
        words2 = [w for w in words1 if (len(w) > 2) is True]

        # Emit each word in the list with count of 1
        for word in words2:
            yield word, 1

    def reducer_wdc(self, word, counts):
        """Reducer counts words occurrence"""
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordCount.run()