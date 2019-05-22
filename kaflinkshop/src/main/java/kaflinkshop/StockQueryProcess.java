package kaflinkshop;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class StockQueryProcess
		extends KeyedProcessFunction<Tuple, Tuple2<String, JsonNode>, String> {

	/**
	 * The state that is maintained by this process function
	 */
	private ValueState<StockState> state;

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("stock_state", StockState.class));
	}

	private transient ObjectMapper jsonParser;

	@Override
	public void processElement(
			Tuple2<String, JsonNode> value,
			Context ctx,
			Collector<String> out) throws Exception {


		String user_id = value.f0;
		JsonNode inputNode = value.f1;
		String route = inputNode.get("route").asText();
		String output;

		switch (route) {
			case "stock/add":
				output = processItemAdd(inputNode);
				break;
			case "stock/subtract":
				output = processItemSubtract(inputNode);
				break;
			case "stock/availability":
				output = processItemAvailability(inputNode);
				break;
			case "stock/item/create":
				output = processItemCreate(inputNode, user_id);
				break;
			default:
				output = responseError(inputNode);
		}

		out.collect(output);

	}

	private ObjectNode createOutput(JsonNode inputNode) {
		if (jsonParser == null) {
			jsonParser = new ObjectMapper();
		}
		ObjectNode jNode = jsonParser.createObjectNode();
		jNode.put("request_id", inputNode.get("request_id"));
		return jNode;
	}

	private String responseSuccess(JsonNode inputNode, StockState current, String message) {
		ObjectNode output = createOutput(inputNode);
		output.put("result", "success");
		output.put("message", message);
		output.put("id", current.id);
		output.put("amount", current.amount);
		return output.toString();
	}

	private String responseError(JsonNode inputNode) {
		ObjectNode output = createOutput(inputNode);
		output.put("error", "Something went wrong!");
		return output.toString();
	}

	private String responseFailure(JsonNode inputNode, String message) {
		ObjectNode output = createOutput(inputNode);
		output.put("result", "failure");
		output.put("message", message);
		return output.toString();
	}

	private String processItemCreate(JsonNode inputNode, String itemId) throws IOException {
		StockState current = new StockState(itemId, 0);
		state.update(current); // write the state back
		return responseSuccess(inputNode, current, "Item created.");
	}

	private String processItemAdd(JsonNode inputNode) throws IOException {
		StockState current = state.value();
		JsonNode params = inputNode.get("params");
		long qty = params.get("number").asLong();

		if (current == null) {
			return responseError(inputNode);
		} else {
			current.amount += qty;
			state.update(current);
			return responseSuccess(inputNode, current, "Added items to stock.");
		}
	}

	private String processItemSubtract(JsonNode inputNode) throws IOException {
		StockState current = state.value();
		JsonNode params = inputNode.get("params");
		long qty = params.get("number").asLong();

		if (current == null) {
			return responseError(inputNode);
		} else {
			if (current.amount >= qty) {
				current.amount -= qty;
				state.update(current);
				return responseSuccess(inputNode, current, "Subtracted items from stock.");
			} else {
				return responseFailure(inputNode, "Not enough items.");
			}
		}
	}

	private String processItemAvailability(JsonNode inputNode) throws IOException {
		StockState current = state.value();

		if (current == null) {
			return responseError(inputNode);
		} else {
			return responseSuccess(inputNode, current, "Item " + (current.amount > 0 ? "available." : "unavailable."));
		}
	}

}
