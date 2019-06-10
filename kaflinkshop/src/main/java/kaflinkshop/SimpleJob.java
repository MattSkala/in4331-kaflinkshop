package kaflinkshop;

import kaflinkshop.User.UserState;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.IOException;

/**
 * Represents basic job implementation.
 */
public class SimpleJob {

	public final JobParams params;

	public SimpleJob(JobParams params) {
		this.params = params;
	}

	public boolean execute() throws IOException {
		// check if all params are given
		if (!this.params.isValid())
			throw new IllegalArgumentException("Params must be filled in.");

		// instantiate the producer
		FlinkKafkaProducer011<Output> producer = new FlinkKafkaProducer011<>(
				params.kafkaAddress,
				params.defaultOutputTopic,
				new DynamicOutputTopicKeyedSerializationSchema(params.defaultOutputTopic));

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
		environment.addSource(new FlinkKafkaConsumer011<>(params.inputTopic, new SimpleStringSchema(), params.properties))
				.uid("source-"+params.inputTopic) // ID for the source operator
				.map(new MessageParser())
				.map(params.keyExtractor)
				.keyBy(new MessageKeySelector())
				.process(params.processFunction)
				.addSink(producer)
				.uid("source-"+params.defaultOutputTopic); // ID for the source operator

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
