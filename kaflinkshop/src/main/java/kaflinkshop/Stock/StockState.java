package kaflinkshop.Stock;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import static kaflinkshop.CommunicationFactory.PARAM_AMOUNT;
import static kaflinkshop.CommunicationFactory.PARAM_ITEM_ID;

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
		node.put(PARAM_ITEM_ID, this.itemID);
		node.put(PARAM_AMOUNT, this.amount);
		return node;
	}

}
