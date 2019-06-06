package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

import static kaflinkshop.CommunicationFactory.*;

public class BatchCollector implements AggregateFunction<Message, BatchCollector.BatchAccumulator, Message> {

	@Override
	public BatchAccumulator createAccumulator() {
		return new BatchAccumulator();
	}

	@Override
	public BatchAccumulator add(Message message, BatchAccumulator accumulator) {
		return accumulator.add(message);
	}

	@Override
	public Message getResult(BatchAccumulator accumulator) {
		return accumulator.getMessage();
	}

	@Override
	public BatchAccumulator merge(BatchAccumulator a, BatchAccumulator b) {
		return BatchAccumulator.merge(a, b);
	}

	static class BatchAccumulator {

		public Map<String, JsonNode> items = new HashMap<>();
		public Message message;

		BatchAccumulator() {
			this.items = new HashMap<>();
			this.message = null;
		}

		BatchAccumulator(Map<String, JsonNode> items, Message message) {
			this.items = items;
			this.message = message;
		}

		BatchAccumulator(BatchAccumulator other) {
			this.items = new HashMap<>();
			this.items.putAll(other.items);
			this.message = other.message;
		}

		BatchAccumulator add(Message message) {
			BatchAccumulator accumulator = new BatchAccumulator(this);

			if (accumulator.message == null)
				accumulator.message = message;

			String itemID = message.params.get(PARAM_ITEM_ID).asText();
			JsonNode pass = message.params.get(PARAM_BATCH_PASS);

			accumulator.items.put(itemID, pass);

			return accumulator;
		}

		Message getMessage() {
			ObjectMapper objectMapper = new ObjectMapper();

			ObjectNode products = objectMapper.createObjectNode();
			this.items.forEach(products::set);

			ObjectNode params = objectMapper.createObjectNode();
			params.put(PARAM_ORDER_ID, this.message.params.get(PARAM_ORDER_ID).asText());
			params.put(PARAM_USER_ID, this.message.params.get(PARAM_USER_ID).asText());
			params.set(PARAM_PRODUCTS, products);

			return Message.redirect(
					this.message,
					SERVICE_STOCK,
					this.message.input.route,
					this.message.state.state,
					params);
		}

		static BatchAccumulator merge(BatchAccumulator a, BatchAccumulator b) {
			HashMap<String, JsonNode> merged = new HashMap<>();
			merged.putAll(a.items);
			merged.putAll(b.items);
			return new BatchAccumulator(merged, a.message);
		}

	}

}
