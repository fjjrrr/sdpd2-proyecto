#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network.
 Usage: structured_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Structured Streaming
   would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py
    localhost 9999`
"""
import sys
import os
from time import sleep

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Set JVM path (OpenJDK 17)
os.environ['JAVA_HOME']="C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.19.10-hotspot"
os.environ['HADOOP_HOME']="C:\\hadoop"
os.environ['PATH']="C:\\hadoop\\bin;" + os.environ.get('PATH', '')

# packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages {0} pyspark-shell".format(packages)
# )

# Load additional packages
conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
conf.set("spark.sql.shuffle.partitions", "1")

if __name__ == "__main__":
    spark = (SparkSession.builder
        .appName("StructuredNetworkWordCount")
        #.remote("sc://localhost:15002")
        .master("local[1]")
        #.master("spark://192.168.1.84:7077")
        .config(conf=conf)
        .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    input_data = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "purchases")
                  .option("startingOffsets", "earliest")
                  .load()
                  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))

    # Stream 1: outputMode append — todos los eventos del stream
    query_append = (input_data.writeStream
                    .queryName("raw_data")
                    .format("memory")
                    .outputMode("append")
                    .start())

    # Stream 2: outputMode complete — agregacion del numero de mensajes por clave
    agg_data = input_data.groupBy("key").agg(count("value").alias("num_mensajes"))

    query_complete = (agg_data.writeStream
                      .queryName("agg_data")
                      .format("memory")
                      .outputMode("complete")
                      .start())

    for x in range(5):
        # Please, note that the 'table' has the same name of the stream variable above
        spark.sql("SELECT * FROM raw_data").show()
        sleep(3)

    # Consulta 1 - todos los eventos del stream (outputMode: append)
    print("\n=== Consulta 1: todos los eventos del stream (outputMode: append) ===")
    resultado1 = spark.sql("""
        SELECT key, value
        FROM raw_data
        ORDER BY key
    """)
    resultado1.show(50, truncate=False)

    with open("salida1.txt", "w", encoding="utf-8") as f:
        f.write("=== Consulta 1: todos los eventos del stream (outputMode: append) ===\n\n")
        for row in resultado1.collect():
            f.write(f"Clave: {row['key']:<20} | Valor: {row['value']}\n")

    # Consulta 2 - numero de mensajes por clave (outputMode: complete)
    print("\n=== Consulta 2: numero de mensajes por clave (outputMode: complete) ===")
    resultado2 = spark.sql("""
        SELECT key, num_mensajes
        FROM agg_data
        ORDER BY num_mensajes DESC
    """)
    resultado2.show(truncate=False)

    with open("salida2.txt", "w", encoding="utf-8") as f:
        f.write("=== Consulta 2: numero de mensajes por clave (outputMode: complete) ===\n\n")
        for row in resultado2.collect():
            f.write(f"Clave: {row['key']:<20} | Mensajes: {row['num_mensajes']:>3}\n")

    # Close Spark session
    query_append.stop()
    query_complete.stop()
    spark.stop()