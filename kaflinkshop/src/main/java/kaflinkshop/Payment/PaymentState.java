package kaflinkshop.Payment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class PaymentState {

	public String orderID;
	public long price;
	public PaymentStatus status;

	PaymentState() {
		orderID = null;
		status = PaymentStatus.INVALID;
		price = -1;
	}

	PaymentState(String orderID) {
		this.orderID = orderID;
		this.status = PaymentStatus.INVALID;
		this.price = -1;
	}

	PaymentState(String orderID, PaymentStatus status) {
		this.orderID = orderID;
		this.status = status;
		this.price = -1;
	}

	PaymentState(String orderID, PaymentStatus status, long price) {
		this.orderID = orderID;
		this.status = status;
		this.price = price;
	}

	public JsonNode toJsonNode(ObjectMapper objectMapper) {
		ObjectNode node = objectMapper.createObjectNode();
		addParams(node);
		return node;
	}

	public void addParams(ObjectNode node) {
		node.put("order_id", this.orderID);
		node.put("status", this.status.name());
		node.put("status_code", this.status.getStatusCode());
	}

}
