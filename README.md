# YouTube Data Analysis with MapReduce
This project demonstrates a simulation of big data analytics using Apache Hadoop and tools in the Hadoop ecosystem.

<details>
  <summary>Table of Contents</summary>
  
  1. Setup
  2. Data Source
  3. Data Preprocessing
     * Part 1
     * Part 2
     * Download Preprocessed Datasets
</details>


# Setup
Compute:
* 3 AWS EC2 intances (1 master node, 2 slave nodes) 
* Instance type: t2 medium

Installations:
* [Apache Hadoop 3.2.2](https://hadoop.apache.org/release/3.2.2.html)
* [Apache Hive 3.1.2](https://hive.apache.org/downloads.html)
* [mrjob](https://mrjob.readthedocs.io/en/latest/)


# Data Source
Kaggle:
* Trending YouTube Video Statistics ([Nov 2017 - Jun 2018](https://www.kaggle.com/datasnaek/youtube-new?select=CAvideos.csv))
* YouTube Trending Video Dataset ([Aug 2020 - Present](https://www.kaggle.com/rsrishav/youtube-trending-video-dataset?select=BR_youtube_trending_data.csv))

_Note: Cutoff date of data collection for this project is 14/06/2021, i.e. data used in this project is only up to 14/06/2021_


# Data Preprocessing
To simulate the application of big data analytics, the following steps are performed to combine the datasets and produce a relatively larger file:

### Part 1
```Data Preparation.ipynb``` prepares data for ```analysis_n_x.py``` <br>
```n```: 1/ 2/ 3 <br>
```y```: mrjob/ norm

**File Level**
1. Resolved 2017-2018 data encoding issue:
    * Converted ANSI encoding to utf-8 using Notepad++ (for Japan, Korea, Mexico and Russia data)
2. Deleted Brazil’s dataset from 2020-2021 data as 2017-2018 data does not contain
dataset for Brazil. This is to maintain the consistency of the countries in both time
periods.

**Data Level**
1. Dropped columns that exist in either one of the dataset but not both datasets.
    * Column dropped for 2017-2018: “video_error_or_removed”
    * Column dropped for 2020-2021: “channelID”
2. Added the column “country” as an identifier for the country that the rows belong to.
3. Standardized the “publishAt” and “publish_time” date format.
4. Merged “category_id” with category extracted from JSON files containing category data. Null values occurred for rows with “category_id” 29, which represented “Nonprofits & Activism”. Thus this category was added during the merging process.
5. Renamed column names and rearranged column positions to ensure both datasets are compatible to append/ concatenate.
6. Combined all datasets and produced the final dataset with approximately 1.5GB of file size.

### Part 2
```Data Preparation 2.ipynb``` prepares data for ```comparison_hive.hql```, ```comparison_mrjob.py```, and ```comparison_norm.py```

1. Similar steps with Part 1.
2. Removed columns “title”, “channelTitle”, “tags”, and “description” to reduce the complexity of importing data with inconsistent delimiters. 
3. Upsampled the dataset by duplicating the data 10 times to retain a relatively larger data file size of approximately 1.5GB.

### Download Preprocessed Datasets
The preprocessed datasets are available for download by running:
```
bash setup.sh
```
