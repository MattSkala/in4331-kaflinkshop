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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;


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
public class PaymentJob {
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String kafkaAddress = "localhost:9092";
		String outputTopic = "payment_out_api1";
		String inputTopic = "payment_in";
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", kafkaAddress);
		properties.setProperty("zookeeper.connect", "localhost:2181");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties));

		FlinkKafkaProducer011<String> flinkKafkaProducer = createProducer(
				outputTopic, kafkaAddress);
		stream.print();

		stream.flatMap(new PaymentJsonParser()).keyBy(0).process(new PaymentQueryProcess()).addSink(flinkKafkaProducer);

		// execute program
		env.execute("Payment streaming job execution");
	}


	private static FlinkKafkaProducer011<String> createProducer(
			String topic, String kafkaAddress) {

		return new FlinkKafkaProducer011<>(kafkaAddress,
				topic, new SimpleStringSchema());
	}
}


