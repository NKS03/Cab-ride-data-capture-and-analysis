# Cab-ride-data-capture-and-analysis

The Project involves capturing data for cab rides for 'ABC' company from different data sources by forming a standard ETL pipeline and then performing some analysis to pull out few metrics for the same 


Details:

Data is coming from 2 sources:
1) AWS RDS is holding the Booking details
2) Click stream data is coming from Kafka.

Tasks:
1) Ingest the data from both the sources to hdfs -> sqoop and pyspark were used.
2) The data were stored in hive tables -> certain data are nested so they need to be flattened with the help of pyspark
3) Perform basic analysis and share the KPIs-> can be seen on the squeries and step documents
