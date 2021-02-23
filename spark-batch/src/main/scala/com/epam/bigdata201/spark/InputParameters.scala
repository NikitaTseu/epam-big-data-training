package com.epam.bigdata201.spark

case class InputParameters(kafkaServers: String = "localhost:9092",
                           kafkaTopic: String = "hotels-with-weather",
                           pathToExpedia: String = "hdfs://localhost:9000/datasets/expedia/*.avro",
                           outputPath: String = "hdfs://localhost:9000/datasets/expedia_valid/")
