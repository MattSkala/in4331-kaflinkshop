package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class PaymentJsonParser implements FlatMapFunction<String, Tuple2<Tuple2<String, String>, JsonNode>> {
	private transient ObjectMapper jsonParser;

	@Override
	public void flatMap(String value, Collector<Tuple2<Tuple2<String, String>, JsonNode>> out) throws Exception {
		if (jsonParser == null) {
			jsonParser = new ObjectMapper();
		}
		JsonNode jsonNode;
		try {
			jsonNode = jsonParser.readValue(value, JsonNode.class);
		} catch (Exception e) {
			System.out.println("Could not be parsed");
			return;
		}
		JsonNode params = jsonNode.get("params");
		String userId;
		String orderId;

		System.out.println(jsonNode.toString());

		if (params.has("user_id") && params.has("order_id")) {
			userId = params.get("user_id").asText();
			orderId = params.get("order_id").asText();
		} else {
			System.out.println("No user or order ID given.");
			return;
		}

		out.collect(new Tuple2<>(new Tuple2<>(userId, orderId), jsonNode));
	}
}

