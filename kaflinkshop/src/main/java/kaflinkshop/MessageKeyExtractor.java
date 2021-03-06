package kaflinkshop;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.Objects;

/**
 * Provides an interface for extracting keys from a given message.
 * <p>
 * Not to be confused with {@link MessageKeySelector} or {@link org.apache.flink.api.java.functions.KeySelector}.
 * The first implements the latter, both only "select" a key given a message. This class, "extracts" the key
 * i.e. it retrieves the appropriate key given a message and some context. In other words, it provides a Message
 * object with an associated key, whereas the two alternatives only link a message and its (previously retrieved)
 * key.
 */
public abstract class MessageKeyExtractor implements MapFunction<Message, Message> {

	public abstract String getKey(Message message);

	@Override
	public Message map(Message message) throws Exception {
		String key = getKey(message);
		Objects.requireNonNull(key);
		message.state.state_id = key;
		return message;
	}

}
