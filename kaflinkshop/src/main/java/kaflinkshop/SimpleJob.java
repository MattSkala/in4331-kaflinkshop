package kaflinkshop;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * Represents basic job implementation.
 */
public class SimpleJob {

	public final JobParams params;

	public SimpleJob(JobParams params) {
		this.params = params;
	}

	public boolean execute() {
		// check if all params are given
		if (!this.params.isValid())
			throw new IllegalArgumentException("Params must be filled in.");

		// instantiate required components
		MessageParser messageParser = new MessageParser();
		FlinkKafkaProducer011<Output> producer = new FlinkKafkaProducer011<>(
				params.kafkaAddress,
				params.defaultOutputTopic,
				new DynamicOutputTopicKeyedSerializationSchema(params.defaultOutputTopic));

		// get the execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// retrieve and process input stream
		DataStream<String> stream = environment.addSource(new FlinkKafkaConsumer011<>(params.inputTopic, new SimpleStringSchema(), params.properties));
		stream.print();

		stream.flatMap(messageParser)
				.flatMap(params.keyExtractor)
				.keyBy(new MessageKeySelector())
				.process(params.processFunction)
				.addSink(producer);

		try {
			// execute program
			environment.execute("Stream execution in progress - listening to topic " + params.inputTopic);
			return true;
		} catch (Exception e) {
			System.out.println("Could not execute environment.");
			e.printStackTrace();
			return false;
		}
	}

	public static class DynamicOutputTopicKeyedSerializationSchema implements KeyedSerializationSchema<Output> {

		public final String defaultOutputTopic;

		public DynamicOutputTopicKeyedSerializationSchema(String defaultOutputTopic) {
			this.defaultOutputTopic = defaultOutputTopic;
		}

		@Override
		public byte[] serializeKey(Output output) {
			return null; // no key is available
		}

		@Override
		public byte[] serializeValue(Output output) {
			return output.getMessage().toString().getBytes();
		}

		@Override
		public String getTargetTopic(Output output) {
			return output.getOutputTopic().orElse(this.defaultOutputTopic);
		}

	}

}
