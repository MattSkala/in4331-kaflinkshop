package kaflinkshop.Stock;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class StockState {

	public String itemID;
	public long amount;

	public StockState() {
		this.itemID = null;
		this.amount = 0;
	}

	public StockState(String itemID) {
		this.itemID = itemID;
		this.amount = 0;
	}

	public StockState(String itemID, long amount) {
		this.itemID = itemID;
		this.amount = amount;
	}

	public JsonNode toJsonNode(ObjectMapper objectMapper) {
		ObjectNode node = objectMapper.createObjectNode();
		node.put("item_id", this.itemID);
		node.put("amount", this.amount);
		return node;
	}

}
