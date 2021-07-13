CREATE TABLE IF NOT EXISTS youtube (video_id string, trending_date string,
    category_id int, category string, country string, publish_time string,
    views int, likes int, dislikes int, comment_count int,
    thumbnail_link string, comments_disabled boolean, ratings_disabled boolean)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hadoop/YouTube/data/combined_filtered_upsampled.csv'
    INTO TABLE youtube;

INSERT OVERWRITE DIRECTORY '/user/hadoop/hiveoutput'
SELECT country, category, count(category)/10 FROM youtube GROUP BY country, category;
