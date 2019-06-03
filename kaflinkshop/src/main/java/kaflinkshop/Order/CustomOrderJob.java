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

package kaflinkshop.Order;

import kaflinkshop.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class CustomOrderJob {

	public static void main(String[] args) {
		// define job params
		JobParams params = new JobParams();
		params.kafkaAddress = CommunicationFactory.KAFKA_DEFAULT_ADDRESS;
		params.inputTopic = CommunicationFactory.ORDER_IN_TOPIC;
		params.defaultOutputTopic = CommunicationFactory.ORDER_OUT_TOPIC;
		params.keyExtractor = new SimpleMessageKeyExtractor("order_id");
		params.processFunction = new OrderQueryProcess();
		params.attachDefaultProperties(CommunicationFactory.ZOOKEEPER_DEFAULT_ADDRESS);

		// check if all params are given
		if (!params.isValid())
			throw new IllegalArgumentException("Params must be filled in.");

		// instantiate required components
		MessageParser messageParser = new MessageParser();
		FlinkKafkaProducer011<Output> producer = new FlinkKafkaProducer011<>(
				params.kafkaAddress,
				params.defaultOutputTopic,
				new SimpleJob.DynamicOutputTopicKeyedSerializationSchema(params.defaultOutputTopic));

		// get the execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// retrieve and process input stream
		DataStream<String> stream = environment.addSource(new FlinkKafkaConsumer011<>(params.inputTopic, new SimpleStringSchema(), params.properties));
		stream.print();

		KeyedStream<Message, String> keyedStream = stream
				.flatMap(messageParser)
				.flatMap(params.keyExtractor)
				.keyBy(new MessageKeySelector());

		/*
		 * HOWTO: here you've got access to the stream. Feel free to process is the way you like.
		 * We may need to process it in different ways. For example, we may need to join two
		 * incoming streams on the same key (like shown in the link below).
		 *
		 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/joining.html
		 *
		 * (The code in this file was copied from kaflinkshop.SimpleJob)
		 */

		keyedStream
				.process(params.processFunction)
				.addSink(producer);

		try {
			// execute program
			environment.execute("Stream execution in progress - listening to topic " + params.inputTopic);
		} catch (Exception e) {
			System.out.println("Could not execute environment.");
			e.printStackTrace();
		}
	}

}


