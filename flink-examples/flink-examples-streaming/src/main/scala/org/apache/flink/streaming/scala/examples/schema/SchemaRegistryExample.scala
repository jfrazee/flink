/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.examples.schema

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * An example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, prefix them by a configured prefix and output to the output topic.
 *
 * Please pass the following arguments to run the example:
 * {{{
 * --input-topic test-input
 * --output-topic test-output
 * --bootstrap.servers localhost:9092
 * --zookeeper.connect localhost:2181
 * --group.id myconsumer
 * }}}
 */
object SchemaRegistryExample {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: SchemaRegistryExample --topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> "
        + "--group.id <some id> "
        + "[--schema-type <type>] [--schema <path>] [--output <path>]")
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000)
    env.getConfig.setGlobalJobParameters(params)

    // Should be able to simplify this but can't find implicit evidence for
    // TypeInformation since AvroSchema is generic
    val messageStream =
      params.get("schema-type", "NONE").toUpperCase match {
        case "GENERIC" => {
          val kafkaConsumer =
            new FlinkKafkaConsumer010(
              params.getRequired("topic"),
              new GenericDataFileDeserializationSchema,
              params.getProperties
            )
          env.addSource(kafkaConsumer).map { x: GenericRecord =>
            x.toString
          }
        }
        case "NONE" => {
          val kafkaConsumer =
            new FlinkKafkaConsumer010(
              params.getRequired("topic"),
              new SimpleStringSchema,
              params.getProperties
            )
          env.addSource(kafkaConsumer)
        }
        case s => {
          println(s"Unknown schema type: ${s}")
          return
        }
      }

    if (params.has("output")) {
      messageStream.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      messageStream.print()
    }

    env.execute("Schema Registry Example")
  }

}
