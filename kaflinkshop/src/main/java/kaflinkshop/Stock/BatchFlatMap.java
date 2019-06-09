package kaflinkshop.Stock;

import kaflinkshop.Message;
import kaflinkshop.ServiceException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static kaflinkshop.CommunicationFactory.*;

public class BatchFlatMap implements FlatMapFunction<Message, Message> {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void flatMap(Message message, Collector<Message> collector) throws Exception {
		if (!message.params.has(PARAM_PRODUCTS))
			throw new ServiceException("Products node is expected to exist.");
		if (!message.params.has(PARAM_ORDER_ID))
			throw new ServiceException("Order ID is expected to exist.");
		if (!message.params.has(PARAM_USER_ID))
			throw new ServiceException("User ID is expected to exist.");

		String returnState = message.params.get(PARAM_RETURN_STATE).asText();
		String orderID = message.params.get(PARAM_ORDER_ID).asText();
		String userID = message.params.get(PARAM_USER_ID).asText();
		String batchID = UUID.randomUUID().toString();
		JsonNode productsNode = message.params.get(PARAM_PRODUCTS);

		int count = 0;
		{
			Iterator<String> iterator = productsNode.fieldNames();
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}
		}

		Iterator<Map.Entry<String, JsonNode>> fields = productsNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> field = fields.next();

			String itemID = field.getKey();
			long amount = field.getValue().asLong();

			ObjectNode params = objectMapper.createObjectNode();
			params.put(PARAM_ITEM_ID, itemID);
			params.put(PARAM_AMOUNT, amount);
			params.put(PARAM_USER_ID, userID);
			params.put(PARAM_ORDER_ID, orderID);
			params.put(PARAM_BATCH_ID, batchID);
			params.put(PARAM_BATCH_COUNT, count);
			params.put(PARAM_RETURN_STATE, returnState);

			Message redirect = Message.redirect(
					message,
					SERVICE_STOCK,
					message.state.route,
					message.state.state,
					params
			);

			collector.collect(redirect);
		}
	}

}
