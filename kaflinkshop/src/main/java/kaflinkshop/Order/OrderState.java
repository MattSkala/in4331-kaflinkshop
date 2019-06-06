package kaflinkshop.Order;

import kaflinkshop.OperationResult;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kaflinkshop.CommunicationFactory.*;

public class OrderState {

	public String orderID;
	public String userID;
	public HashMap<String, Integer> products;
	public boolean userChecked;
	public boolean isPaid;
	public OrderCheckoutStatus checkoutStatus = OrderCheckoutStatus.NOT_PROCESSED;
	public int checkoutProgress = OrderCheckoutProgress.NOT_PROCESSED;
	public List<CheckoutMessage> checkoutMessages;
	public Map<String, Long> checkoutStock;

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
		fillJsonNode(node, objectMapper);
		return node;
	}

	public void fillJsonNode(ObjectNode node, ObjectMapper objectMapper) {
		node.put(PARAM_ORDER_ID, this.orderID);
		node.put(PARAM_USER_ID, this.userID);
		node.put(PARAM_USER_CHECKED, this.userChecked);
		node.put(PARAM_ORDER_PAID, this.isPaid);
		node.set(PARAM_PRODUCTS, getProductsAsJson(objectMapper));
		node.put(PARAM_ORDER_CHECKOUT_STATUS, this.checkoutStatus.getCode());
		node.put(PARAM_ORDER_CHECKOUT_PROGRESS, this.checkoutProgress);
	}

	public JsonNode getProductsAsJson(ObjectMapper objectMapper) {
		ObjectNode products = objectMapper.createObjectNode();
		this.products.forEach(products::put);
		return products;
	}

	public JsonNode getCheckoutStockAsJson(ObjectMapper objectMapper) {
		ObjectNode products = objectMapper.createObjectNode();
		this.checkoutStock.forEach(products::put);
		return products;
	}

	public JsonNode getChecoutMessagesAsJson(ObjectMapper objectMapper) {
		ArrayNode array = objectMapper.createArrayNode();
		for (CheckoutMessage message : this.checkoutMessages) {
			ObjectNode node = objectMapper.createObjectNode();
			node.put("message", message.message);
			node.put("status", message.result.toString());
			node.put("status_code", message.result.getCode());
			array.add(node);
		}
		return array;
	}

	public long getPrice() {
		long amount = 0;
		for (Integer qty : products.values())
			amount += qty;
		return amount;
	}


	public static class CheckoutMessage {

		public String message;
		public OperationResult result;

		public CheckoutMessage(OperationResult result, String message) {
			this.result = result;
			this.message = message;
		}

	}

}
