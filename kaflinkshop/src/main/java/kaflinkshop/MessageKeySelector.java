package kaflinkshop;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.UUID;

public class MessageKeySelector implements KeySelector<Message, String> {

	public final String param;

	public MessageKeySelector(String param) {
		this.param = param;
	}

	@Override
	public String getKey(Message message) throws Exception {
		if (message.params.has(this.param)) {
			return message.params.get(this.param).asText();
		} else {
			return UUID.randomUUID().toString();
		}
	}

}
