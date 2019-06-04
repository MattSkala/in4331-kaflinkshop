package kaflinkshop;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Parses JSON to a {@link Message} object and sets the state ID.
 */
public class MessageParser implements MapFunction<String, Message> {

	@Override
	public Message map(String input) throws Exception {
		try {
			return Message.parse(input);
		} catch (Exception e) {
			System.out.println("Could not parse message.");
			throw e;
		}
	}

}
