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

package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.UUID;



/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class UserJob {
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String filebackend = "file:///root/Documents/TU_Delft/WebData/rocksDB/";
		String savebackend = "file:///root/Documents/TU_Delft/WebData/saveDB/";

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, filebackend);

		/*
		Task local recovery can be enabled, the idea is here:
		https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/large_state_tuning.html
		 */

		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
		config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savebackend);

		RocksDBStateBackendFactory factory = new RocksDBStateBackendFactory();
		StateBackend backend = factory.createFromConfig(config, null);

		env.enableCheckpointing(10000);
		env.setStateBackend(backend);

		String kafkaAddress = "localhost:9092";
		String outputTopic = "user_out_api1";
		String inputTopic = "user_in";
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaAddress);
		properties.setProperty("zookeeper.connect", "localhost:2181");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties));

		FlinkKafkaProducer011<String> flinkKafkaProducer = createProducer(
				outputTopic, kafkaAddress);

		stream.flatMap(new Splitter()).keyBy(0).process(new UserQueryProcess()).addSink(flinkKafkaProducer);

		// execute program
		env.execute("User streaming job execution");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, JsonNode>> {
		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, JsonNode>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode;
			try {
				jsonNode = jsonParser.readValue(value, JsonNode.class);
			} catch(Exception e){
				System.out.println("Could not be parsed");
				return;
			}
			JsonNode params = jsonNode.get("params");
			String user_id;

			if(params.has("user_id")){
//				System.out.println("Getting used key");
				user_id = params.get("user_id").asText();
			} else {
//				System.out.println("Creating new key");
				user_id = UUID.randomUUID().toString();
			}
			out.collect(new Tuple2<>(user_id, jsonNode));
		}
	}

	private static FlinkKafkaProducer011<String> createProducer(
			String topic, String kafkaAddress){

		return new FlinkKafkaProducer011<>(kafkaAddress,
				topic, new SimpleStringSchema());
	}
}


