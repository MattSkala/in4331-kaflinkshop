package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.api.java.functions.KeySelector;

public class BatchKeySelector implements KeySelector<Message, String> {

	@Override
	public String getKey(Message message) throws Exception {
		return message.params.get("batch_id").asText();
	}

}
