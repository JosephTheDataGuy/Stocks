# Stocks
The purpose of this repo is to create a resource that utilizes SEC data and various news API to pull pertanent data regarding user input of a ticker symbol.

Originally, the project was designed to use AWS RDS and a MySQL hosted database. After successfully setting it up, I was hit with a $1.80 bill for the month, having exceeded the free tier. This bill scared me from using the cloud for the service as I do not really have a budget for this project. Therefore, I swithced to using PostgresSQL 16 hosted on my local machine for my database.

Moving forward, the goal is to set up a system to pull a ticker symbol's data dependent on user input, and set up a cron job to automatically pull and store the data on a daily basis.

