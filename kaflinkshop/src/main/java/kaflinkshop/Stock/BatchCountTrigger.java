package kaflinkshop.Stock;

import kaflinkshop.Message;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BatchCountTrigger extends Trigger<Message, Window> {

	private static final long serialVersionUID = 1L;
	private final ReducingStateDescriptor<Long> stateDesc;

	public BatchCountTrigger() {
		this.stateDesc = new ReducingStateDescriptor<Long>("count", new BatchCountTrigger.Sum(), LongSerializer.INSTANCE);
	}

	public TriggerResult onElement(Message message, long timestamp, Window window, TriggerContext ctx) throws Exception {
		ReducingState<Long> count = ctx.getPartitionedState(this.stateDesc);
		count.add(1L);

		if (!message.params.has("batch_count"))
			throw new IllegalStateException("Message should contain field 'batch_count'.");
		long maxCount = message.params.get("batch_count").asLong();

		if ((Long) count.get() >= maxCount) {
			count.clear();
			return TriggerResult.FIRE;
		} else {
			return TriggerResult.CONTINUE;
		}
	}

	public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) {
		return TriggerResult.CONTINUE;
	}

	public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	public void clear(Window window, TriggerContext ctx) throws Exception {
		((ReducingState) ctx.getPartitionedState(this.stateDesc)).clear();
	}

	public boolean canMerge() {
		return true;
	}

	public void onMerge(Window window, OnMergeContext ctx) throws Exception {
		ctx.mergePartitionedState(this.stateDesc);
	}

	public String toString() {
		return "BatchCountTrigger";
	}

	private static class Sum implements ReduceFunction<Long> {

		private Sum() {
		}

		public Long reduce(Long a, Long b) throws Exception {
			return a + b;
		}

	}
}
