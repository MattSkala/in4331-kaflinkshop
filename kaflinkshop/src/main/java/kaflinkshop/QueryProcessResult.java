package kaflinkshop;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Provides an abstraction that allows ease of use for {@link QueryProcess} for simple services.
 */
public interface QueryProcessResult {

	Output createOutput(Message original, String serviceName);

	class Redirect implements QueryProcessResult {
		public String topic;
		public String route;
		public JsonNode params;
		public String state;

		public Redirect(@Nonnull String topic, @Nonnull String route, @Nonnull JsonNode params, @Nullable String state) {
			Objects.requireNonNull(topic);
			Objects.requireNonNull(route);
			Objects.requireNonNull(params);
			this.route = route;
			this.params = params;
			this.state = state;
		}

		public Redirect(@Nonnull String topic, @Nonnull String route, @Nonnull JsonNode params) {
			this(topic, route, params, null);
		}

		@Override
		public Output createOutput(Message original, String serviceName) {
			Message message = Message.redirect(original, serviceName, this.route, this.state, this.params);
			return new Output(message, this.topic);
		}
	}

	class Success implements QueryProcessResult {
		public JsonNode params;
		public String message;

		public Success() {
		}

		public Success(@Nullable JsonNode params) {
			this.params = params;
		}

		public Success(@Nullable String message) {
			this.message = message;
		}

		public Success(@Nullable JsonNode params, @Nullable String message) {
			this.params = params;
			this.message = message;
		}

		@Override
		public Output createOutput(Message original, String serviceName) {
			return new Output(Message.success(original, serviceName, this.message, this.params));
		}
	}

	class Failure implements QueryProcessResult {

		public JsonNode params;
		public String message;

		public Failure() {
		}

		public Failure(@Nullable JsonNode params) {
			this.params = params;
		}

		public Failure(@Nullable JsonNode params, @Nullable String message) {
			this.params = params;
			this.message = message;
		}

		@Override
		public Output createOutput(Message original, String serviceName) {
			return new Output(Message.failure(original, serviceName, this.message, this.params));
		}
	}

}
