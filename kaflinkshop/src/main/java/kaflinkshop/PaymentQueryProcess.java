package kaflinkshop;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class PaymentQueryProcess
		extends KeyedProcessFunction<Tuple, Tuple2<Tuple2<String, String>, JsonNode>, String> {

	/**
	 * The state that is maintained by this process function
	 */
	private ValueState<PaymentState> state;

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("payment_state", PaymentState.class));
	}

	private transient ObjectMapper jsonParser;

	@Override
	public void processElement(
			Tuple2<Tuple2<String, String>, JsonNode> value,
			Context ctx,
			Collector<String> out) throws Exception {


		String userId = value.f0.f0;
		String orderId = value.f0.f1;
		JsonNode inputNode = value.f1;
		String route = inputNode.get("route").asText();
		String output;

		switch (route) {
			case "payment/pay":
				output = processPay(inputNode);
				break;
			case "payment/cancelPayment":
				output = processCancel(inputNode);
				break;
			case "payment/status":
				output = processStatus(inputNode);
				break;
			default:
				output = responseError(inputNode);
		}

		out.collect(output);
	}

	private ObjectNode createOutput(JsonNode inputNode) {
		if (jsonParser == null) {
			jsonParser = new ObjectMapper();
		}
		ObjectNode jNode = jsonParser.createObjectNode();
		jNode.put("request_id", inputNode.get("request_id"));
		return jNode;
	}

	private String responseSuccess(JsonNode inputNode, PaymentState current, String message) {
		ObjectNode output = createOutput(inputNode);
		output.put("result", "success");
		output.put("message", message);
		output.put("statusCode", current.status.getStatusCode());
		output.put("status", current.status.name());
		return output.toString();
	}

	private String responseError(JsonNode inputNode) {
		ObjectNode output = createOutput(inputNode);
		output.put("error", "Something went wrong!");
		return output.toString();
	}

	private String responseFailure(JsonNode inputNode, String message) {
		ObjectNode output = createOutput(inputNode);
		output.put("result", "failure");
		output.put("message", message);
		return output.toString();
	}

	private String processPay(JsonNode inputNode) throws IOException {
		PaymentState current = state.value();

		// TODO: implement payment
		// This will chang ein future, with call to other services.
		// For now, let's just simulate instant yes-no replies.
		// We first mark the payment as PAID and then

		if (current == null) {
			System.out.println("Starting payment process...");

			if (Math.random() >= 0.5) {
				current = new PaymentState(PaymentStatus.PAID);
				state.update(current);
				return responseSuccess(inputNode, current, "Payment successful.");
			} else {
				current = new PaymentState(PaymentStatus.CANCELLED);
				state.update(current);
				return responseFailure(inputNode, "Payment unsuccessful.");
			}
		} else {
			// The state should not be set, if it is, this means we have already issued a payment for the given user and order.
			return responseError(inputNode);
		}
	}

	private String processCancel(JsonNode inputNode) throws IOException {
		PaymentState current = state.value();

		if (current == null) {
			return responseError(inputNode);
		} else {
			if (current.status == PaymentStatus.PAID) {
				current.status = PaymentStatus.CANCELLED;
				state.update(current);
				return responseSuccess(inputNode, current, "Payment cancelled successfully.");
			} else {
				return responseFailure(inputNode, "Payment cannot be cancelled.");
			}
		}

	}

	private String processStatus(JsonNode inputNode) throws IOException {
		PaymentState current = state.value();

		if (current == null) {
			return responseError(inputNode);
		} else {
			return responseSuccess(inputNode, current, "Payment status retrieved.");
		}
	}

}
