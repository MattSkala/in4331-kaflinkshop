package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import static kaflinkshop.CommunicationFactory.*;

public class BatchItem extends Message {

	public String batchID;
	public String itemID;
	public long amount;

	public BatchItem() {
		super();
	}

	public BatchItem(Message message, String itemID, String batchID, long amount) {
		super(message.input, message.state, message.params, message.path, message.result);
		this.itemID = itemID;
		this.batchID = batchID;
		this.amount = amount;
	}

	public static BatchItem redirectMessage(Message original, String itemID, String batchID, long amount, String route) {
		ObjectNode params = new ObjectMapper().createObjectNode();
		params.put(PARAM_ITEM_ID, itemID);
		params.put(PARAM_BATCH_ID, batchID);
		params.put(PARAM_AMOUNT, amount);

		Message message = Message.redirect(
				original,
				SERVICE_STOCK,
				route,
				original.state.state,
				params);
		message.state.state_id = itemID;

		return new BatchItem(message, itemID, batchID, amount);
	}

}
