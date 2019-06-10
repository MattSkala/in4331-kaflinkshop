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

package kaflinkshop.Stock;

import kaflinkshop.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.IOException;

import static kaflinkshop.CommunicationFactory.ORDER_IN_TOPIC;
import static kaflinkshop.CommunicationFactory.STOCK_IN_TOPIC;

public class StockJob {

	public static void main(String[] args) throws IOException {
		// define job params
		JobParams params = new JobParams();
		params.kafkaAddress = CommunicationFactory.KAFKA_DEFAULT_ADDRESS;
		params.inputTopic = CommunicationFactory.STOCK_IN_TOPIC;
		params.defaultOutputTopic = CommunicationFactory.STOCK_OUT_TOPIC;
		params.keyExtractor = new SimpleMessageKeyExtractor(CommunicationFactory.PARAM_ITEM_ID);
		params.processFunction = new StockQueryProcess();
		params.attachDefaultProperties(CommunicationFactory.ZOOKEEPER_DEFAULT_ADDRESS);

		// check if all params are given
		if (!params.isValid())
			throw new IllegalStateException("Params must be filled in.");

		// instantiate the producer
		FlinkKafkaProducer011<Output> producer = new FlinkKafkaProducer011<>(
				params.kafkaAddress,
				params.defaultOutputTopic,
				new SimpleJob.DynamicOutputTopicKeyedSerializationSchema(params.defaultOutputTopic));

		// get the execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// Checkpointing and saving
		String filebackend = "file:///rocksDBcheck" + params.defaultOutputTopic + "/";
		String savebackend = "file:///rocksDBsave" + params.defaultOutputTopic + "/";
		CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, filebackend);
		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
		config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savebackend);
		RocksDBStateBackendFactory factory = new RocksDBStateBackendFactory();
		StateBackend backend = factory.createFromConfig(config, null);
		environment.enableCheckpointing(10000);
		environment.setStateBackend(backend);

		// retrieve and process input stream
		SingleOutputStreamOperator<Message> stream = environment.addSource(new FlinkKafkaConsumer011<>(params.inputTopic, new SimpleStringSchema(), params.properties))
				.map(new MessageParser());

		// the non-batch stream
		DataStream<Message> normalStream = stream
				.filter(new BatchFilter(false));

		// the batch stream
		DataStream<Message> batchStream = stream
				.filter(new BatchFilter(true))
				.flatMap(new BatchFlatMap());

		// both streams (normal & batch) merged back together
		DataStream<Message> mergedStream = normalStream.union(batchStream);

		// process this merged stream
		DataStream<Output> processedStream = mergedStream
				.map(params.keyExtractor)
				.keyBy(new MessageKeySelector())
				.process(params.processFunction);

		// split the merged stream back to batch stream ...
		processedStream
				.filter(new OutputTopicFilter(STOCK_IN_TOPIC, true, false))
				.map(Output::getMessage)
				.filter(new BatchFilter(true))
				.keyBy(new BatchKeySelector())
				.timeWindow(Time.seconds(10))
				.trigger(new BatchCountTrigger())
				.aggregate(new BatchCollector())
				.map(t -> new Output(t, ORDER_IN_TOPIC))
				.addSink(producer);

		// ... and normal stream.
		processedStream
				.filter(new OutputTopicFilter(STOCK_IN_TOPIC, false, true))
				.addSink(producer);

		try {
			// execute program
			environment.execute("Stream execution in progress - listening to topic " + params.inputTopic);
		} catch (
				Exception e) {
			System.out.println("Could not execute environment.");
			e.printStackTrace();
		}
	}

}


