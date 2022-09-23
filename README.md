# twitter-sentiment-analysis

`Data pipeline collecting random tweets from Twitter with Java and sending them into Kafka, then collecting from Kafka and transforming them with PySpark and finally sending to MongoDB as raw data and to PostgreSQL as processed data.`

## General information

This Data Pipeline aims to classify the sentiment of tweets retrieved in real time. The data is retrieved from the Twitter API with Java every minute and is sent to a Kafka broker, in this example the tweets include the word "coronavirus" but it can easily be modified.

The classification and processing of tweets is done on PySpark. Before any analysis, the tweets are sent to a NoSQL MongoDB database in order to keep a trace of the raw data. The tweets are then classified with the TextBlob library and the classification tables are sent to PostgreSQL.

## Visual explanation

Fig1. Visual explanation of how the data pipeline works
![streaming](https://user-images.githubusercontent.com/94069984/191958643-960d07b5-90a3-43a9-873b-cdcc1eadd719.jpg)

## Requirements

Conditions to launch the code:
* Java with Maven
* Zookeeper
* Kafka
* PySpark
* MongoDB
* PostgreSQL
