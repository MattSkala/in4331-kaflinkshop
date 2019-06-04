package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.calcite.plan.RelOptListener;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import javax.sound.midi.Soundbank;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static kaflinkshop.CommunicationFactory.*;

public class BatchFlatMap implements FlatMapFunction<Message, Message> {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void flatMap(Message message, Collector<Message> collector) throws Exception {
		String batchID = UUID.randomUUID().toString();
		String orderID = message.params.get(PARAM_ORDER_ID).asText();
		String userID = message.params.get(PARAM_ORDER_ID).asText();
		JsonNode productsNode = message.params.get("products");

		if (productsNode == null) {
			String item_id = message.params.get("item_id").asText();
			System.out.println("production node is null: " + item_id);
			return;
		}

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
			params.put("batch_id", batchID);
			params.put("batch_count", count);

			Message redirect = Message.redirect(
					message,
					SERVICE_STOCK,
					"batch/validate",
					message.state.state,
					params
			);

			collector.collect(redirect);
		}
	}

}
