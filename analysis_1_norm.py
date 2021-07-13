#!/usr/bin/env python
# coding: utf-8

import csv

# List of country of interest
country_list = ["Canada", "Great Britain", "United States"]
unique_title = []

# Read csv file
with open('./data/combined_all.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, quoting=csv.QUOTE_ALL, skipinitialspace=True)

    for row in reader:
        # Filter country
        if row[6] in country_list:
            title = row[2]  # Extract the title column

            # Check for unique video titles
            if title not in unique_title:
                unique_title.append(title)

words = []

for title in unique_title:
    # Remove leading and trailing whitespace
    title = title.strip()

    # Standardize to lowercase
    lowercase = title.lower()

    # Tokenize title into single words
    temp = lowercase.split()

    # Append words to list
    for word in temp:
        words.append(word)

# Assign stopwords list to variable stops
stops = ["it's", "they", "with", "himself", "will", "being", "couldn't", "shouldn't", "through", "just",
         "i", "more", "very", "my", "its", "who", "her", "below", "t", "ve", "shouldn", "whom", "out", "to",
         "yourselves", "your", "has", "that'll", "mustn't", "these", "the", "over", "hasn't", "she", "in",
         "are", "didn", "further", "both", "now", "own", "weren't", "for", "all", "not", "should've", "how",
         "on", "some", "needn't", "hers", "up", "there", "him", "ll", "under", "themselves", "is", "wasn",
         "you've", "where", "mustn", "than", "herself", "you're", "m", "those", "doing", "was", "before",
         "most", "s", "and", "any", "we", "here", "wouldn", "hadn't", "of", "ourselves", "as", "did", "them",
         "can", "a", "ain", "only", "down", "have", "didn't", "an", "off", "other", "then", "she's", "against",
         "re", "having", "yours", "same", "ma", "theirs", "by", "his", "while", "such", "what", "mightn't", "during",
         "this", "each", "yourself", "which", "myself", "hasn", "were", "why", "into", "above", "won't", "haven",
         "their", "until", "it", "or", "if", "be", "aren't", "after", "about", "d", "couldn", "shan", "o", "doesn't",
         "haven't", "at", "between", "so", "been", "me", "nor", "won", "don", "isn't", "do", "should", "few", "you'd",
         "ours", "does", "he", "no", "you", "shan't", "once", "wouldn't", "wasn't", "don't", "our", "am", "but",
         "because", "you'll", "from", "doesn", "isn", "too", "weren", "needn", "y", "itself", "when", "aren", "again",
         "had", "hadn", "mightn", "that"]

# Remove punctuations and numbers
word1 = [w for w in words if w.isalpha()]

# Remove stopwords
word2 = [w for w in word1 if not w in stops]

# Remove words with less than 2 characters
word3 = [w for w in word2 if (len(w) > 2) is True]

# Perform wordcount and store in dict
count_dict = {i: word3.count(i) for i in word3}

# Write output to csv file
with open('./output/analysis_1_output.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in count_dict.items():
        writer.writerow([key, value])
