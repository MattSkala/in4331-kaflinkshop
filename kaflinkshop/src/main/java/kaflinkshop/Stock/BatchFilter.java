package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.api.common.functions.FilterFunction;

public class BatchFilter implements FilterFunction<Message> {

	public boolean batch;

	public BatchFilter(boolean extractBatch) {
		this.batch = extractBatch;
	}

	@Override
	public boolean filter(Message message) throws Exception {
		boolean stateIsBatch = message.state.state != null && message.state.state.startsWith("batch:");
		return stateIsBatch == batch;
	}

}
