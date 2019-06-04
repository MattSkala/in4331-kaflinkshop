package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.api.java.functions.KeySelector;

import static kaflinkshop.CommunicationFactory.PARAM_BATCH_ID;

public class BatchKeySelector implements KeySelector<Message, String> {

	@Override
	public String getKey(Message message) throws Exception {
		return message.params.get(PARAM_BATCH_ID).asText();
	}

}
