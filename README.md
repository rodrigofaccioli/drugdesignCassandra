# drugdesignCassandra
Cassandra Database for drugdesign project
Plase, see https://github.com/rodrigofaccioli/drugdesign for more information about drugdesign project.

# Apache Spark
This project requires Apache Spark 2.0 or higher. See http://spark.apache.org/downloads.html

# How to install spark-cassandra-connector

git clone https://github.com/datastax/spark-cassandra-connector.git
cd spark-cassandra-connector
sbt/sbt -Dscala-2.11=true assembly

# How to run
/Users/rodrigo/programs/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --jars /Users/rodrigo/programs/spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-1.6.4-143-g4a4ae757.jar src/populate_database.py config.json
