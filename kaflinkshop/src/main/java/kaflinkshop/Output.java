package kaflinkshop;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class Output {

	@Nullable
	private final String outputTopic;
	@Nonnull
	private final Message message;

	public Optional<String> getOutputTopic() {
		return Optional.ofNullable(this.outputTopic);
	}

	public Message getMessage() {
		return this.message;
	}

	public Output(Message message) {
		this.message = message;
		this.outputTopic = null;
	}

	public Output(@Nonnull Message message, @Nullable String outputTopic) {
		Objects.requireNonNull(message);
		this.message = message;
		this.outputTopic = outputTopic;
	}

}
