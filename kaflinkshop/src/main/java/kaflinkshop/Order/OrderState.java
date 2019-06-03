package kaflinkshop.Order;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;

public class OrderState {

	public String orderID;
	public String userID;
	public HashMap<String, Integer> products;
	public boolean userChecked;
	public boolean isPaid;

	public OrderState() {
		this.products = new HashMap<>();
	}

	public OrderState(String orderID, String userID) {
		this.orderID = orderID;
		this.userID = userID;
		this.products = new HashMap<>();
	}

	public JsonNode toJsonNode(ObjectMapper objectMapper) {
		ObjectNode products = objectMapper.createObjectNode();
		this.products.forEach(products::put);

		ObjectNode node = objectMapper.createObjectNode();
		node.put("order_id", this.orderID);
		node.put("user_id", this.userID);
		node.put("user_checked", this.userChecked);
		node.put("is_paid", this.isPaid);
		node.set("products", products);

		return node;
	}

	public long countTotalItems() {
		long amount = 0;
		for (Integer qty : products.values())
			amount += qty;
		return amount;
	}

}
