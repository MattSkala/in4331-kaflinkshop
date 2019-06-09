package kaflinkshop;

import java.util.Objects;
import java.util.UUID;

/**
 * Extracts the key form a message. The key is extracted from the message's {@code params}
 * field. If the specified param does not exist, a new UUID is created and returned.
 */
public class SimpleMessageKeyExtractor extends MessageKeyExtractor {

	public final String paramID;

	public SimpleMessageKeyExtractor(String paramID) {
		Objects.requireNonNull(paramID);
		this.paramID = paramID;
	}

	@Override
	public String getKey(Message message) {
		Objects.requireNonNull(message);
		if (message.params.has(paramID)) {
			return message.params.get(paramID).asText();
		} else {
			return UUID.randomUUID().toString();
		}
	}

}
