package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * Parses JSON to a {@link Message} object.
 */
public class MessageParser implements FlatMapFunction<String, Message> {

	@Override
	public void flatMap(String input, Collector<Message> collector) throws Exception {
		try {
			Message message = Message.parse(input);
			collector.collect(message);
		} catch (Exception e) {
			System.out.println("Could not parse message.");
			e.printStackTrace();
		}
	}

}
