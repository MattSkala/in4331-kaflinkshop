package kaflinkshop;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Retrieves ("selects") a key given a message. The key is expected to be present in
 * field {@code message.state.state_id}. This does not "provide" the key, only "retrieves" it.
 */
public class MessageKeySelector implements KeySelector<Message, String> {

	@Override
	public String getKey(Message message) {
		if (message.state.state_id == null) {
			throw new IllegalStateException("Message's state ID should be set at this point.");
		}
		return message.state.state_id;
	}

}
