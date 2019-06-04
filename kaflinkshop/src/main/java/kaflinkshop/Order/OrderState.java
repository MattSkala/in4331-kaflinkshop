package kaflinkshop.Order;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;

import static kaflinkshop.CommunicationFactory.*;

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
		ObjectNode node = objectMapper.createObjectNode();
		node.put(PARAM_ORDER_ID, this.orderID);
		node.put(PARAM_USER_ID, this.userID);
		node.put(PARAM_USER_CHECKED, this.userChecked);
		node.put(PARAM_ORDER_PAID, this.isPaid);
		node.set(PARAM_PRODUCTS, getProductsAsJson(objectMapper));
		return node;
	}

	public JsonNode getProductsAsJson(ObjectMapper objectMapper) {
		ObjectNode products = objectMapper.createObjectNode();
		this.products.forEach(products::put);
		return products;
	}

	public long countTotalItems() {
		long amount = 0;
		for (Integer qty : products.values())
			amount += qty;
		return amount;
	}

}
