package kaflinkshop;

import org.apache.flink.api.common.functions.FilterFunction;

public class OutputTopicFilter implements FilterFunction<Output> {

	public final String topic;
	public final boolean includeTopic;
	public final boolean includeNoTopic;

	public OutputTopicFilter(String topic, boolean includeTopic, boolean includeNoTopic) {
		this.topic = topic;
		this.includeTopic = includeTopic;
		this.includeNoTopic = includeNoTopic;
	}

	@Override
	public boolean filter(Output output) throws Exception {
		return output
				.getOutputTopic()
				.map(t -> t.equals(topic) == includeTopic)
				.orElse(includeNoTopic);
	}

}
