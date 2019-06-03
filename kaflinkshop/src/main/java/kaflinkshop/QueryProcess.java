package kaflinkshop;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Provides an abstract wrapper for simpler implementation of base services.
 */
public abstract class QueryProcess extends KeyedProcessFunction<String, Message, Output> {

	public final String serviceName;

	public QueryProcess(String serviceName) {
		this.serviceName = serviceName;
	}

	@Override
	public void processElement(Message message, Context context, Collector<Output> collector) throws Exception {
		try {
			List<QueryProcessResult> results = this.processElement(message, context);
			for (QueryProcessResult result : results) {
				Output output = result.createOutput(message, this.serviceName);
				collector.collect(output);
			}
		} catch (ServiceException e) {
			Output output = new Output(Message.error(message, this.serviceName, e));
			collector.collect(output);
		} catch (Exception e) {
			Output output = new Output(Message.error(message, this.serviceName, e));
			collector.collect(output);
		}
	}

	abstract public List<QueryProcessResult> processElement(Message message, Context context) throws Exception;

}
