package kaflinkshop;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Provides an abstract wrapper for simpler implementation of base services.
 *
 * @param <T> Key type.
 */
public abstract class QueryProcess<T> extends KeyedProcessFunction<T, Message, Output> {

	public final String serviceName;

	public QueryProcess(String serviceName) {
		this.serviceName = serviceName;
	}

	@Override
	public void processElement(Message message, Context context, Collector<Output> collector) throws Exception {
		Output output;

		try {
			QueryProcessResult result = this.processElement(message, context);
			output = result.createOutput(message, this.serviceName);
		} catch (ServiceException e) {
			output = new Output(Message.error(message, this.serviceName, e));
		} catch (Exception e) {
			output = new Output(Message.error(message, this.serviceName, e));
		}

		collector.collect(output);
	}

	abstract public QueryProcessResult processElement(Message message, Context context) throws Exception;

}
