package kaflinkshop;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

public class JobParams<T> {

	public String kafkaAddress;
	public String inputTopic;
	public String defaultOutputTopic;
	public Properties properties;
	public KeySelector<Message, T> keySelector;
	public KeyedProcessFunction<T, Message, Output> processFunction;

	public void attachDefaultProperties(String bootstrapServers, String zookeeperConnect) {
		this.properties = new Properties();
		this.properties.setProperty("bootstrap.servers", bootstrapServers);
		this.properties.setProperty("zookeeper.connect", "localhost:2181");
	}

	public void attachDefaultProperties(String zookeeperConnect) {
		if (this.kafkaAddress == null)
			throw new IllegalStateException("kafkaAddress must be given or specified.");
		this.properties = new Properties();
		this.properties.setProperty("bootstrap.servers", this.kafkaAddress);
		this.properties.setProperty("zookeeper.connect", "localhost:2181");
	}

	public boolean isValid() {
		return Stream.of(
				kafkaAddress,
				inputTopic,
				properties,
				defaultOutputTopic,
				keySelector,
				processFunction)
				.allMatch(Objects::nonNull);
	}

}
