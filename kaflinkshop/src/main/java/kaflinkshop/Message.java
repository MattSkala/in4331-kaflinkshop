package kaflinkshop;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.codehaus.commons.nullanalysis.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Message {

	public static class MessageInput {
		public String request_id;
		public String consumer;
		public String route;
		public JsonNode params;
	}

	public static class State {
		public String route;
		public String sender;
		@Nullable
		public String state;
		@Nullable
		public String state_id;
	}

	public static class PathPoint {
		public String consumer;
		public String route;
		@Nullable
		public String state;
		@Nullable
		public String state_id;
		@Nullable
		public JsonNode params;
	}

	public static class Result {
		public String result;
		@Nullable
		public String message;
		@Nullable
		public JsonNode params;
	}

	public static final String RESULT_TYPE_SUCCESS = "success";
	public static final String RESULT_TYPE_FAILURE = "failure";
	public static final String RESULT_TYPE_ERROR = "error";

	public MessageInput input;
	public State state;
	public JsonNode params;
	@Nullable
	public Result result;
	public List<PathPoint> path;

	Message() {
		ObjectMapper objectMapper = new ObjectMapper();
		params = objectMapper.createObjectNode();
		this.path = new ArrayList<>();
	}

	Message(MessageInput input, State state, JsonNode params, List<PathPoint> path, @Nullable Result result) {
		this.input = input;
		this.state = state;
		this.params = params;
		this.path = path == null ? new ArrayList<>() : path;
		this.result = result;
	}

	public ObjectNode toObjectNode() {
		ObjectMapper objectMapper = new ObjectMapper();

		ObjectNode root = objectMapper.createObjectNode();
		ObjectNode input = objectMapper.createObjectNode();
		ObjectNode state = objectMapper.createObjectNode();
		ObjectNode result = objectMapper.createObjectNode();
		ArrayNode path = objectMapper.createArrayNode();

		if (this.input != null) {
			input.put("request_id", this.input.request_id);
			input.put("consumer", this.input.consumer);
			input.put("route", this.input.route);
			input.set("params", this.input.params);
		}

		if (this.state != null) {
			state.put("route", this.state.route);
			state.put("sender", this.state.sender);
			state.put("state", this.state.state);
			state.put("state_id", this.state.state_id);
		}

		if (this.result != null) {
			result.put("result", this.result.result);
			result.put("message", this.result.message);
			result.set("params", this.result.params);
		}

		for (PathPoint pathPoint : this.path) {
			ObjectNode node = objectMapper.createObjectNode();
			node.put("consumer", pathPoint.consumer);
			node.put("route", pathPoint.consumer);
			node.put("state", pathPoint.state);
			node.put("state_id", pathPoint.state_id);
			node.set("params", pathPoint.params);
			path.add(node);
		}

		root.set("input", input);
		root.set("state", state);
		root.set("params", params);
		root.set("path", path);
		root.set("result", result);

		return root;
	}

	public String toString() {
		return toObjectNode().toString();
	}

	private static @Nullable
	String toString(JsonNode node) {
		if (node == null || node.isNull())
			return null;
		else
			return node.asText();
	}

	public static Message parse(JsonNode node) {
		try {
			JsonNode params = null;
			if (node.has("params")) {
				params = node.get("params");
			}

			MessageInput input = null;
			if (node.has("input")) {
				JsonNode nodeInput = node.get("input");
				input = new MessageInput();
				input.consumer = toString(nodeInput.get("consumer"));
				input.params = nodeInput.get("params");
				input.request_id = toString(nodeInput.get("request_id"));
				input.route = toString(nodeInput.get("route"));
			}

			State state = null;
			if (node.has("state")) {
				JsonNode nodeState = node.get("state");
				state = new State();
				state.route = toString(nodeState.get("route"));
				state.sender = toString(nodeState.get("sender"));
				state.state = toString(nodeState.get("state"));
				state.state_id = toString(nodeState.get("state_id"));
			}

			Result result = null;
			if (node.has("result")) {
				JsonNode nodeResult = node.get("result");
				result = new Result();
				result.message = toString(nodeResult.get("message"));
				result.result = toString(nodeResult.get("result"));
				result.params = nodeResult.get("params");
			}

			List<PathPoint> path = new ArrayList<>();
			if (node.has("path")) {
				node.get("path").elements().forEachRemaining(nodePathPoint -> {
					PathPoint pathPoint = new PathPoint();
					pathPoint.consumer = toString(nodePathPoint.get("consumer"));
					pathPoint.route = toString(nodePathPoint.get("route"));
					pathPoint.state = toString(nodePathPoint.get("state"));
					pathPoint.state_id = toString(nodePathPoint.get("state_id"));
					pathPoint.params = nodePathPoint.get("params");
					path.add(pathPoint);
				});
			}

			return new Message(input, state, params, path, result);
		} catch (Exception e) {
			System.out.println("Error parsing node.");
			e.printStackTrace();
			throw new IllegalArgumentException("Unknown message format.");
		}
	}

	public static Message parse(String value) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			JsonNode node = objectMapper.readValue(value, JsonNode.class);
			return parse(node);
		} catch (IOException e) {
			System.out.println("Error parsing message");
			e.printStackTrace();
			throw new IllegalArgumentException("Unknown message format.");
		}
	}

	public Message deepCopy() {
		MessageInput input = null;
		if (this.input != null) {
			input = new MessageInput();
			input.route = this.input.route;
			input.request_id = this.input.request_id;
			input.consumer = this.input.consumer;
			input.params = this.input.params.deepCopy();
		}

		State state = null;
		if (this.state != null) {
			state = new State();
			state.state = this.state.state;
			state.sender = this.state.sender;
			state.route = this.state.route;
			state.state_id = this.state.state_id;
		}

		Result result = null;
		if (this.result != null) {
			result = new Result();
			result.result = this.result.result;
			result.message = this.result.message;
			result.params = this.result.params.deepCopy();
		}

		ArrayList<PathPoint> path = new ArrayList<>(this.path.size());
		for (PathPoint pp : this.path) {
			PathPoint pathPoint = new PathPoint();
			pathPoint.state = pp.state;
			pathPoint.state_id = pp.state_id;
			pathPoint.route = pp.route;
			pathPoint.consumer = pp.consumer;
			pathPoint.params = pp.params.deepCopy();
			path.add(pathPoint);
		}

		JsonNode params = null;
		if (this.params != null) {
			params = this.params.deepCopy();
		}

		return new Message(input, state, params, path, result);
	}

	public static Message.PathPoint pathPointFromMessageState(Message message, String serviceName) {
		Message.PathPoint pathPoint = new Message.PathPoint();
		pathPoint.params = message.params;
		pathPoint.consumer = serviceName;
		pathPoint.route = message.state.route;
		pathPoint.state = message.state.state;
		pathPoint.state_id = message.state.state_id;
		return pathPoint;
	}

	public static Message redirect(
			@Nonnull Message original,
			@NotNull String serviceName,
			@Nonnull String route,
			@Nullable String state,
			@Nullable JsonNode params
	) {
		Message message = original.deepCopy();

		Message.PathPoint pathPoint = pathPointFromMessageState(message, serviceName);
		message.path.add(pathPoint);

		message.state.route = route;
		message.state.sender = serviceName;
		message.state.state = state;
		message.state.state_id = null;

		message.params = params;

		message.result = null;

		return message;
	}

	public static Message result(
			@Nonnull Message original,
			@Nonnull String serviceName,
			String result,
			@Nullable String msg,
			@Nullable JsonNode params
	) {
		Message message = original.deepCopy();

		Message.PathPoint pathPoint = pathPointFromMessageState(message, serviceName);
		message.path.add(pathPoint);

		message.state.route = "web";
		message.state.sender = serviceName;
		message.state.state = null;
		message.state.state_id = null;

		message.params = null;

		message.result = new Message.Result();
		message.result.message = msg;
		message.result.result = result;
		message.result.params = params;

		return message;
	}

	public static Message success(
			@Nonnull Message original,
			@Nonnull String serviceName,
			@Nullable String msg,
			@Nullable JsonNode params
	) {
		return result(original, serviceName, RESULT_TYPE_SUCCESS, msg, params);
	}

	public static Message failure(
			@Nonnull Message original,
			@Nonnull String serviceName,
			@Nullable String msg,
			@Nullable JsonNode params
	) {
		return result(original, serviceName, RESULT_TYPE_FAILURE, msg, params);
	}

	public static Message error(
			@Nonnull Message original,
			@Nonnull String serviceName,
			@Nullable String msg,
			@Nullable JsonNode params
	) {
		return result(original, serviceName, RESULT_TYPE_ERROR, msg, params);
	}

	private static JsonNode paramsFromException(Exception exception) {
		ObjectMapper objectMapper = new ObjectMapper();

		ArrayNode trace = objectMapper.createArrayNode();
		for (StackTraceElement t : exception.getStackTrace()) {
			trace.add(t.toString());
		}

		ObjectNode params = objectMapper.createObjectNode();
		params.put("type", exception.getClass().toString());
		params.put("info", exception.toString());
		params.set("trace", trace);

		return params;
	}

	public static Message error(
			@Nonnull Message original,
			@Nonnull String serviceName,
			@Nonnull ServiceException serviceException
	) {
		String msg = "Managed Exception: " + serviceException.getMessage();
		JsonNode params = paramsFromException(serviceException);
		return result(original, serviceName, RESULT_TYPE_ERROR, msg, params);
	}

	public static Message error(
			@Nonnull Message original,
			@Nonnull String serviceName,
			@Nonnull Exception exception
	) {
		String msg = "Unmanaged Exception: " + exception.getMessage();
		JsonNode params = paramsFromException(exception);
		return result(original, serviceName, RESULT_TYPE_ERROR, msg, params);
	}

}
