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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import java.time.LocalDateTime;
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
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String kafkaAddress = "localhost:9092";
		String outputTopic = "user_out";
		String inputTopic = "user_in";

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaAddress);
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "kaflinkshop");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties));

		FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(
				outputTopic, kafkaAddress);

		stream.flatMap(new Splitter()).keyBy(0).process(new UserQueryProcess()).addSink(flinkKafkaProducer);
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		stream.print();

		// execute program
		env.execute("User streaming job execution");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple3<String, String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple3<String, String, String>> out) throws Exception {
			String[] split_input = sentence.split(" ");
			String id = "";
			String action = split_input[0];
			String name = "";
			if(action.equals("create_user")){
				id = UUID.randomUUID().toString();
				name = split_input[1];
			} else if(action.equals("get_user")){
				id = split_input[1];
			}
			out.collect(new Tuple3<>(id, action, name));
		}
	}

	public static FlinkKafkaProducer011<String> createStringProducer(
			String topic, String kafkaAddress){

		return new FlinkKafkaProducer011<>(kafkaAddress,
				topic, new SimpleStringSchema());
	}
}


