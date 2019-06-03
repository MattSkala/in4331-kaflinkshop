package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Parses JSON to a {@link Message} object and sets the state ID.
 */
public class MessageParser implements FlatMapFunction<String, Message> {

	@Override
	public void flatMap(String input, Collector<Message> collector) {
		try {
			Message message = Message.parse(input);
			collector.collect(message);
		} catch (Exception e) {
			System.out.println("Could not parse message.");
			e.printStackTrace();
		}
	}

}
